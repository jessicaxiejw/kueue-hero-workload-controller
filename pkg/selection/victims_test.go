// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package selection

import (
	"testing"
	"time"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

func TestGroupVictimsDropsDangling(t *testing.T) {
	d := domain("b1", 128, 0, "gone", "present")
	wls := wlMap(victimWL("present", 100, "cq", 4, time.Hour))
	got := GroupVictims(d, wls, nil, now)
	if len(got) != 1 || got[0].Key.Name != "present" {
		t.Errorf("GroupVictims = %+v, want only 'present'", got)
	}
}

func TestGroupVictimsAdmittedCount(t *testing.T) {
	d := domain("b1", 128, 0, "partial")
	// Admitted at 4 pods (partial admission), spec would say 10.
	wl := victimWL("partial", 100, "cq", 4, time.Hour)
	wl.Spec.PodSets = []kueue.PodSet{{Name: "main", Count: 10}}
	got := GroupVictims(d, wlMap(wl), nil, now)
	if len(got) != 1 || got[0].PodCount != 4 {
		t.Errorf("PodCount = %+v, want admitted 4 not spec 10", got)
	}
	if got[0].RunningFor != time.Hour {
		t.Errorf("RunningFor = %v, want 1h", got[0].RunningFor)
	}
}
