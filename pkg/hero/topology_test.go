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
			wl:   heroWorkload().Obj(),
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

func TestGroupingLevel(t *testing.T) {
	hierarchy := []string{levelBlock, levelRack, levelHost} // highest -> lowest

	cases := []struct {
		name       string
		wl         *kueue.Workload
		drainLevel string
		want       string
		wantOK     bool
	}{
		{
			// The shape that motivated this: slices each need a rack, but
			// nothing ties the racks together, so the drain packs across
			// every rack in the cluster rather than one block's worth.
			name: "slice pair without required leaves the drain unconstrained",
			wl: utiltesting.MakeWorkload("slices", "ns").PodSets(
				*utiltesting.MakePodSet("workers", 30).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(5).Obj(),
			).Obj(),
			drainLevel: levelRack,
			want:       "",
			wantOK:     true,
		},
		{
			name: "required above the drain level groups the drain",
			wl: utiltesting.MakeWorkload("both", "ns").PodSets(
				*utiltesting.MakePodSet("workers", 30).
					RequiredTopologyRequest(levelBlock).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(5).Obj(),
			).Obj(),
			drainLevel: levelRack,
			want:       levelBlock,
			wantOK:     true,
		},
		{
			// Finer than the drain level: satisfied inside any single
			// drained domain, so it groups nothing.
			name: "required below the drain level constrains nothing",
			wl: utiltesting.MakeWorkload("fine", "ns").PodSets(
				*utiltesting.MakePodSet("workers", 30).
					RequiredTopologyRequest(levelHost).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(5).Obj(),
			).Obj(),
			drainLevel: levelRack,
			want:       "",
			wantOK:     true,
		},
		{
			name: "required level outside the hierarchy is a misconfiguration",
			wl: utiltesting.MakeWorkload("bogus", "ns").PodSets(
				*utiltesting.MakePodSet("workers", 30).
					RequiredTopologyRequest("example.com/nope").
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(5).Obj(),
			).Obj(),
			drainLevel: levelRack,
			wantOK:     false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := GroupingLevel(tc.wl, hierarchy, tc.drainLevel)
			if ok != tc.wantOK || (ok && got != tc.want) {
				t.Errorf("GroupingLevel = %q, %v; want %q, %v", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestGroupingLevelsExtraction(t *testing.T) {
	cases := []struct {
		name string
		wl   *kueue.Workload
		want []string
	}{
		{
			// The common hero shape: slices must each fit a rack, but
			// nothing ties the racks together, so the drain may spread.
			name: "slice pair without required yields no grouping",
			wl: utiltesting.MakeWorkload("slices", "ns").PodSets(
				*utiltesting.MakePodSet("workers", 30).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(5).Obj(),
			).Obj(),
			want: nil,
		},
		{
			name: "required alongside the slice pair groups the drain",
			wl: utiltesting.MakeWorkload("both", "ns").PodSets(
				*utiltesting.MakePodSet("workers", 30).
					RequiredTopologyRequest(levelBlock).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(5).Obj(),
			).Obj(),
			want: []string{levelBlock},
		},
		{
			// Only podsets that contribute demand can constrain the plan;
			// a required-only podset drives no chunks.
			name: "required without the slice pair is ignored",
			wl:   heroWorkload().Obj(),
			want: nil,
		},
		{
			name: "preferred is not a grouping constraint",
			wl: utiltesting.MakeWorkload("preferred", "ns").PodSets(
				*utiltesting.MakePodSet("workers", 30).
					PreferredTopologyRequest(levelBlock).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(5).Obj(),
			).Obj(),
			want: nil,
		},
		{
			name: "distinct required levels deduped in podset order",
			wl: utiltesting.MakeWorkload("jobset", "ns").PodSets(
				*utiltesting.MakePodSet("leader", 2).
					RequiredTopologyRequest(levelBlock).
					SliceRequiredTopologyRequest(levelRack).
					SliceSizeTopologyRequest(2).Obj(),
				*utiltesting.MakePodSet("workers", 16).
					RequiredTopologyRequest(levelRack).
					SliceRequiredTopologyRequest(levelHost).
					SliceSizeTopologyRequest(8).Obj(),
			).Obj(),
			want: []string{levelBlock, levelRack},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := groupingLevels(tc.wl)
			if len(got) != len(tc.want) {
				t.Fatalf("groupingLevels = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("groupingLevels = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestFinestLevelHelper(t *testing.T) {
	hierarchy := []string{levelBlock, levelRack, levelHost} // highest -> lowest

	cases := []struct {
		name     string
		required []string
		want     string
		wantOK   bool
	}{
		{name: "single level", required: []string{levelRack}, want: levelRack, wantOK: true},
		{
			// Mirror of CoarsestLevel: a rack sits inside a block, so
			// grouping by rack honours the block requirement too.
			name:     "disagreement picks finest",
			required: []string{levelRack, levelBlock},
			want:     levelRack,
			wantOK:   true,
		},
		{name: "hostname is finest", required: []string{levelHost, levelBlock}, want: levelHost, wantOK: true},
		{name: "level not in topology", required: []string{"example.com/not-a-level"}, wantOK: false},
		{name: "one of several not in topology", required: []string{levelBlock, "example.com/nope"}, wantOK: false},
		{name: "empty required", required: nil, wantOK: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := finestLevel(tc.required, hierarchy)
			if ok != tc.wantOK || (ok && got != tc.want) {
				t.Errorf("finestLevel(%v) = %q, %v; want %q, %v", tc.required, got, ok, tc.want, tc.wantOK)
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
