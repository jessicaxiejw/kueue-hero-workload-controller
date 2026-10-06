// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package selection

import (
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/snapshot"
)

// VictimWorkload is a Kueue workload that draining a given domain would
// evict, joined with the fields the cost formula consumes. Eviction is
// all-or-nothing per workload, so pods are grouped by owning workload.
type VictimWorkload struct {
	Key          types.NamespacedName
	ClusterQueue string
	Cohort       string
	Priority     int32
	// PodCount is the ADMITTED pod count (podSetAssignments[].count) —
	// not spec count; partial admission (minCount) makes them differ.
	PodCount int32
	// RunningFor is time since the workload was admitted.
	RunningFor time.Duration
}

// GroupVictims folds a domain's per-pod victims into per-workload entries,
// joining against the Workload objects. Pods whose workload is missing
// (already deleted — a benign race) are dropped: they are vacating anyway.
// A workload's cost fields (priority, admitted pod count, age) describe the
// WHOLE workload, not its footprint in this domain — eviction kills all of
// it regardless of where the rest runs.
func GroupVictims(
	d *snapshot.Domain,
	workloads map[types.NamespacedName]*kueue.Workload,
	cohorts map[string]string,
	now time.Time,
) []VictimWorkload {
	seen := map[types.NamespacedName]bool{}
	out := make([]VictimWorkload, 0, len(d.Victims))
	for i := range d.Victims {
		key := d.Victims[i].Workload
		if seen[key] {
			continue
		}
		seen[key] = true
		wl, ok := workloads[key]
		if !ok {
			continue
		}
		out = append(out, VictimWorkload{
			Key:          key,
			ClusterQueue: admittedClusterQueue(wl),
			Cohort:       cohorts[admittedClusterQueue(wl)], // OK if empty
			Priority:     specPriority(wl),
			PodCount:     admittedPodCount(wl),
			RunningFor:   runningFor(wl, now),
		})
	}
	return out
}

func admittedClusterQueue(wl *kueue.Workload) string {
	if wl.Status.Admission != nil {
		return string(wl.Status.Admission.ClusterQueue)
	}
	return ""
}

func specPriority(wl *kueue.Workload) int32 {
	if wl.Spec.Priority != nil {
		return *wl.Spec.Priority
	}
	return 0
}

// admittedPodCount sums podSetAssignments[].count — the pods actually
// admitted (partial admission can reduce below spec count). Falls back to
// spec counts when the assignment carries no count.
func admittedPodCount(wl *kueue.Workload) int32 {
	var total int32
	if wl.Status.Admission != nil {
		for i := range wl.Status.Admission.PodSetAssignments {
			if c := wl.Status.Admission.PodSetAssignments[i].Count; c != nil {
				total += *c
			}
		}
	}
	if total > 0 {
		return total
	}
	for i := range wl.Spec.PodSets {
		total += wl.Spec.PodSets[i].Count
	}
	return total
}

// runningFor measures age from the Admitted=True transition (falling back
// to QuotaReserved, then creation) — the design doc's runningSeconds.
func runningFor(wl *kueue.Workload, now time.Time) time.Duration {
	for _, condType := range []string{kueue.WorkloadAdmitted, kueue.WorkloadQuotaReserved} {
		if c := meta.FindStatusCondition(wl.Status.Conditions, condType); c != nil && c.Status == metav1.ConditionTrue {
			return now.Sub(c.LastTransitionTime.Time)
		}
	}
	return now.Sub(wl.CreationTimestamp.Time)
}
