package openreports

import (
	"context"
	"fmt"
	"time"

	openreports "github.com/openreports/reports-api/apis/openreports.io/v1alpha1"
	fleetv1 "github.com/rancher/fleet/pkg/apis/fleet.cattle.io/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	// SourceFleet identifies Fleet as the report producer.
	SourceFleet = "fleet"

	// PolicyConvergence is the rule family for "did what I deployed actually land".
	PolicyConvergence = "fleet-convergence"

	// Rule names. These are the stable identifiers a consumer can alert on.
	RuleResourcePresent     = "resource-present"
	RuleResourceOwned       = "resource-owned"
	RuleResourceUnmodified  = "resource-unmodified"
	RuleNoOrphanedResources = "no-orphaned-resources"
	RuleResourceReady       = "resource-ready"
)

// The reports-api module defines Result and ResultSeverity as bare string
// types with no exported constants, so the allowed values are spelled out
// here. Values come from the OpenReports schema enum.
const (
	ResultPass openreports.Result = "pass"
	ResultFail openreports.Result = "fail"

	SeverityCritical openreports.ResultSeverity = "critical"
	SeverityHigh     openreports.ResultSeverity = "high"
	SeverityMedium   openreports.ResultSeverity = "medium"
	SeverityLow      openreports.ResultSeverity = "low"
)

// CreateOrPatchReport writes the given Report, creating it if absent and
// patching it in place if it already exists.
//
// Reports are level-triggered: one per BundleDeployment, rewritten on every
// reconcile so it always reflects current state. It is never appended to.
func CreateOrPatchReport(ctx context.Context, c client.Client, report *openreports.Report) error {
	existing := &openreports.Report{ObjectMeta: metav1.ObjectMeta{
		Name:      report.Name,
		Namespace: report.Namespace,
	}}

	_, err := controllerutil.CreateOrPatch(ctx, c, existing, func() error {
		existing.Labels = report.Labels
		existing.OwnerReferences = report.OwnerReferences
		existing.Source = report.Source
		existing.Scope = report.Scope
		existing.Summary = report.Summary
		existing.Results = report.Results

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create or patch report %s/%s: %w", report.Namespace, report.Name, err)
	}

	return nil
}

// BuildReport maps a BundleDeployment's status onto an OpenReports Report.
//
// Everything is derived from bd; nothing is looked up. The reconciler already
// holds the object, and drift has already been computed into bd.Status.
func BuildReport(bd *fleetv1.BundleDeployment) *openreports.Report {
	report := &openreports.Report{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bd.Name,
			Namespace: bd.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": SourceFleet,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(bd, fleetv1.SchemeGroupVersion.WithKind("BundleDeployment")),
			},
		},
		Source: SourceFleet,
		Scope: &corev1.ObjectReference{
			APIVersion: fleetv1.SchemeGroupVersion.String(),
			Kind:       "BundleDeployment",
			Name:       bd.Name,
			Namespace:  bd.Namespace,
			UID:        bd.UID,
		},
	}

	// One timestamp for the whole report: every result describes the same
	// observation, so they should not differ by a few microseconds.
	now := metav1.Timestamp{Seconds: time.Now().Unix()}

	// Drift: either name each drifted resource, or record a single pass, so a
	// converged BundleDeployment is distinguishable from one never checked.
	if len(bd.Status.ModifiedStatus) > 0 {
		for _, m := range bd.Status.ModifiedStatus {
			report.Results = append(report.Results, modifiedResult(m, now))
		}
	} else if bd.Status.NonModified {
		report.Results = append(report.Results, ReadyResult(RuleResourceUnmodified, "no drift detected", now))
	}

	// Readiness: same shape.
	if len(bd.Status.NonReadyStatus) > 0 {
		for _, n := range bd.Status.NonReadyStatus {
			report.Results = append(report.Results, nonReadyResult(n, now))
		}
	} else if bd.Status.Ready {
		report.Results = append(report.Results, ReadyResult(RuleResourceReady, "all resources ready", now))
	}
	for i := range report.Results {
		switch report.Results[i].Result {
		case ResultFail:
			report.Summary.Fail++
		case ResultPass:
			report.Summary.Pass++
		}
	}

	return report
}

// modifiedResult turns one drifted/missing/extra resource into a result.
// The rule name is chosen by which combination of booleans Fleet set --
// the same distinction ModifiedStatus.String() makes when rendering a message.
func modifiedResult(m fleetv1.ModifiedStatus, ts metav1.Timestamp) openreports.ReportResult {
	rule := RuleResourceUnmodified
	severity := SeverityMedium

	switch {
	case m.Create && m.Exist:
		// Resource exists but Fleet does not own it: two controllers fighting.
		rule = RuleResourceOwned
		severity = SeverityCritical
	case m.Create:
		rule = RuleResourcePresent
		severity = SeverityHigh
	case m.Delete:
		rule = RuleNoOrphanedResources
		severity = SeverityLow
	}

	result := openreports.ReportResult{
		Source:      SourceFleet,
		Policy:      PolicyConvergence,
		Rule:        rule,
		Result:      ResultFail,
		Severity:    severity,
		Category:    "convergence",
		Scored:      true,
		Timestamp:   ts,
		Description: m.String(),
		Subjects: []corev1.ObjectReference{{
			APIVersion: m.APIVersion,
			Kind:       m.Kind,
			Name:       m.Name,
			Namespace:  m.Namespace,
		}},
	}

	if m.Patch != "" {
		result.Properties = map[string]string{"patch": m.Patch}
	}

	return result
}

// nonReadyResult turns one not-ready resource into a result.
func nonReadyResult(n fleetv1.NonReadyStatus, ts metav1.Timestamp) openreports.ReportResult {
	return openreports.ReportResult{
		Source:      SourceFleet,
		Policy:      PolicyConvergence,
		Rule:        RuleResourceReady,
		Result:      ResultFail,
		Severity:    SeverityHigh,
		Category:    "convergence",
		Scored:      true,
		Timestamp:   ts,
		Description: n.String(),
		Subjects: []corev1.ObjectReference{{
			APIVersion: n.APIVersion,
			Kind:       n.Kind,
			Name:       n.Name,
			Namespace:  n.Namespace,
			UID:        n.UID,
		}},
	}
}

// ReadyResult reports that a whole-BundleDeployment check succeeded.
//
// Unlike a failure, a pass has no single offending resource to point at: the
// statement is about the BundleDeployment as a whole, which Scope already
// names. Subjects is optional in the schema, so it is left unset.
func ReadyResult(rule string, description string, ts metav1.Timestamp) openreports.ReportResult {
	return openreports.ReportResult{
		Source:      SourceFleet,
		Policy:      PolicyConvergence,
		Rule:        rule,
		Result:      ResultPass,
		Category:    "convergence",
		Scored:      true,
		Timestamp:   ts,
		Description: description,
	}
}
