// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package hero

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// pendingSince returns a hero whose QuotaReserved=False condition has the
// given reason and last changed at t.
func pendingSince(reason string, t time.Time) *kueue.Workload {
	c := pendingCondition(reason, "")
	c.LastTransitionTime = metav1.NewTime(t)
	return heroWorkload().Condition(c).Obj()
}

// kueue >= 0.19 flips the reason of a pending hero between cycles. IsPending
// must hold, and PendingFor must keep counting, through every flip.
func TestPendingSurvivesReasonFlips(t *testing.T) {
	t0 := time.Date(2026, 11, 25, 5, 26, 23, 0, time.UTC)
	now := t0.Add(10 * time.Minute)
	for _, reason := range []string{
		"NoMatchingFlavor", "WaitingForQuota", "WaitingForPreemptedWorkloads", "TopologyPlacementFailed", "Pending",
	} {
		wl := pendingSince(reason, t0)
		if !IsPending(wl) {
			t.Errorf("IsPending(%s) = false, want true", reason)
		}
		if got := PendingFor(wl, now); got != 10*time.Minute {
			t.Errorf("PendingFor(%s) = %v, want 10m", reason, got)
		}
	}
}

func TestIsPendingFalseWhenNotWaiting(t *testing.T) {
	t0 := time.Now()
	admitted := pendingSince("WaitingForQuota", t0)
	admitted.Status.Admission = &kueue.Admission{ClusterQueue: "cq"}

	finished := heroWorkload().
		Condition(pendingCondition("WaitingForQuota", "")).
		Condition(metav1.Condition{Type: kueue.WorkloadFinished, Status: metav1.ConditionTrue, Reason: "Succeeded"}).Obj()

	reserved := heroWorkload().Condition(metav1.Condition{
		Type: kueue.WorkloadQuotaReserved, Status: metav1.ConditionTrue, Reason: "QuotaReserved",
	}).Obj()

	for name, wl := range map[string]*kueue.Workload{
		"admitted": admitted, "finished": finished, "quota reserved": reserved, "no condition": heroWorkload().Obj(),
	} {
		if IsPending(wl) {
			t.Errorf("IsPending(%s) = true, want false", name)
		}
		if got := PendingFor(wl, t0.Add(time.Hour)); got != 0 {
			t.Errorf("PendingFor(%s) = %v, want 0", name, got)
		}
	}
}

func TestPendingForClampsClockSkew(t *testing.T) {
	t0 := time.Date(2026, 11, 25, 5, 0, 0, 0, time.UTC)
	if got := PendingFor(pendingSince("WaitingForQuota", t0), t0.Add(-time.Second)); got != 0 {
		t.Errorf("PendingFor with now before transition = %v, want 0", got)
	}
}
