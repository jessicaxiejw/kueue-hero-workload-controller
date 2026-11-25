// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package drainctrl

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
)

func TestStuckStateDebounce(t *testing.T) {
	t0 := time.Date(2026, 11, 25, 5, 26, 23, 0, time.UTC)
	cfg := config.Default() // StuckAfter 2m, detection auto
	r := &Reconciler{Cfg: &cfg}

	wl := &kueue.Workload{}
	wl.Status.Conditions = []metav1.Condition{{
		Type: kueue.WorkloadQuotaReserved, Status: metav1.ConditionFalse,
		Reason: "NoMatchingFlavor", LastTransitionTime: metav1.NewTime(t0),
	}}

	for _, tc := range []struct {
		name      string
		now       time.Time
		reason    string
		wantStuck bool
		wantWait  time.Duration
	}{
		{"just queued waits out the debounce", t0.Add(30 * time.Second), "NoMatchingFlavor", false, 90 * time.Second},
		{"old enough is stuck", t0.Add(2 * time.Minute), "NoMatchingFlavor", true, 0},
		{"reason flip does not reset the age", t0.Add(10 * time.Minute), "WaitingForQuota", true, 0},
		{"kueue already preempting never starts a drain", t0.Add(10 * time.Minute), "WaitingForPreemptedWorkloads", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wl.Status.Conditions[0].Reason = tc.reason
			r.Now = func() time.Time { return tc.now }
			stuck, wait := r.stuckState(wl)
			if stuck != tc.wantStuck || wait != tc.wantWait {
				t.Errorf("stuckState = (%v, %v), want (%v, %v)", stuck, wait, tc.wantStuck, tc.wantWait)
			}
		})
	}

	t.Run("zero stuckAfter disables the debounce", func(t *testing.T) {
		cfg.StuckAfter.Duration = 0
		r.Now = func() time.Time { return t0 }
		wl.Status.Conditions[0].Reason = "NoMatchingFlavor"
		if stuck, wait := r.stuckState(wl); !stuck || wait != 0 {
			t.Errorf("stuckState = (%v, %v), want (true, 0)", stuck, wait)
		}
	})
}
