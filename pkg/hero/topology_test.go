// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package hero

import (
	"testing"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
)

func TestRequiredTopologyLevels(t *testing.T) {
	cases := []struct {
		name string
		wl   *kueue.Workload
		want []string
	}{
		{
			name: "single podset with slice pair",
			wl: utiltesting.MakeWorkload("slices", "ns").PodSets(
				*utiltesting.MakePodSet("workers", 30).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(5).Obj(),
			).Obj(),
			want: []string{levelRack},
		},
		{
			// Plain required is deliberately NOT a drain trigger: the
			// customer contract is slice-required + slice-size.
			name: "plain required does not trigger",
			wl: utiltesting.MakeWorkload("plain-required", "ns").PodSets(
				*utiltesting.MakePodSet("main", 16).
					RequiredTopologyRequest(levelBlock).Obj(),
			).Obj(),
			want: nil,
		},
		{
			name: "no requests at all",
			wl: utiltesting.MakeWorkload("plain", "ns").
				PodSets(*utiltesting.MakePodSet("main", 2).Obj()).Obj(),
			want: nil,
		},
		{
			name: "preferred does not count as required",
			wl: utiltesting.MakeWorkload("preferred", "ns").
				PodSets(*utiltesting.MakePodSet("main", 2).
					PreferredTopologyRequest(levelBlock).Obj()).Obj(),
			want: nil,
		},
		{
			// JobSet shape: kueue builds one podset per ReplicatedJob,
			// each annotating independently. A podset without the slice
			// pair is skipped.
			name: "jobset with unannotated leader",
			wl: utiltesting.MakeWorkload("jobset", "ns").PodSets(
				*utiltesting.MakePodSet("leader", 1).Obj(),
				*utiltesting.MakePodSet("workers", 16).
					SliceRequiredTopologyRequest(levelBlock).
					SliceSizeTopologyRequest(8).Obj(),
			).Obj(),
			want: []string{levelBlock},
		},
		{
			name: "jobset with disagreeing slice levels, deduped in podset order",
			wl: utiltesting.MakeWorkload("jobset", "ns").PodSets(
				*utiltesting.MakePodSet("leader", 2).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(2).Obj(),
				*utiltesting.MakePodSet("workers", 16).
					SliceRequiredTopologyRequest(levelBlock).
					SliceSizeTopologyRequest(8).Obj(),
			).Obj(),
			want: []string{levelRack, levelBlock},
		},
		{
			// Defensive: slice-required without slice-size is invalid
			// (kueue's webhook rejects the annotation pair split), but a
			// Workload written directly could carry it — not a trigger.
			name: "slice-required without slice-size does not count",
			wl: utiltesting.MakeWorkload("half-slice", "ns").PodSets(
				*utiltesting.MakePodSet("workers", 30).
					SliceRequiredTopologyRequest(levelRack).Obj(),
			).Obj(),
			want: nil,
		},
		{
			// Both set: only the slice pair matters — required is not a
			// trigger, so the SLICE level drives the drain.
			name: "slice level drives even when required also set",
			wl: utiltesting.MakeWorkload("both", "ns").PodSets(
				*utiltesting.MakePodSet("workers", 30).
					RequiredTopologyRequest(levelBlock).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(5).Obj(),
			).Obj(),
			want: []string{levelRack},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RequiredTopologyLevels(tc.wl)
			if len(got) != len(tc.want) {
				t.Fatalf("RequiredTopologyLevels = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("RequiredTopologyLevels = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestCoarsestLevel(t *testing.T) {
	hierarchy := []string{levelBlock, levelRack, levelHost} // highest -> lowest

	cases := []struct {
		name     string
		required []string
		want     string
		wantOK   bool
	}{
		{name: "single level", required: []string{levelRack}, want: levelRack, wantOK: true},
		{
			// Leader wants rack, workers want block: drain a whole block —
			// an emptied block satisfies the rack requirement, not vice versa.
			name:     "disagreement picks coarsest",
			required: []string{levelRack, levelBlock},
			want:     levelBlock,
			wantOK:   true,
		},
		{name: "hostname is finest", required: []string{levelHost, levelRack}, want: levelRack, wantOK: true},
		{name: "level not in topology", required: []string{"example.com/not-a-level"}, wantOK: false},
		{name: "one of several not in topology", required: []string{levelBlock, "example.com/nope"}, wantOK: false},
		{name: "empty required", required: nil, wantOK: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := CoarsestLevel(tc.required, hierarchy)
			if ok != tc.wantOK || (ok && got != tc.want) {
				t.Errorf("CoarsestLevel(%v) = %q, %v; want %q, %v", tc.required, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}
