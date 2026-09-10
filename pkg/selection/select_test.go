// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package selection

import (
	"math"
	"testing"
	"time"

	pkghero "github.com/coreweave/kueue-hero-workload-controller/pkg/hero"
	"k8s.io/apimachinery/pkg/api/resource"
)

const bigDomain = "big"

func TestSelectSingleDomain(t *testing.T) {
	c := cfg()
	h := heroSpec(128, 1) // required-style: one 128-GPU chunk

	wls := wlMap(
		victimWL("cheap", 100, heroCQ, 2, time.Minute),
		victimWL("pricey", 900, otherCQ, 200, 40*time.Hour),
	)

	s := snap(
		domain("b1", 256, 0, "pricey"), // feasible, expensive victims
		domain("b2", 256, 0, "cheap"),  // feasible, cheap victims
		domain("b3", 64, 0),            // too small for the chunk
	)

	plan, reason := SelectDomains(s, h, wls, c, now)
	if plan == nil {
		t.Fatalf("outcome = %s", reason)
	}
	if len(plan.DomainIDs) != 1 || plan.DomainIDs[0] != "b2" {
		t.Errorf("selected %v, want [b2]", plan.DomainIDs)
	}
	if len(plan.Victims) != 1 || plan.Victims[0].Key.Name != "cheap" {
		t.Errorf("victims = %+v", plan.Victims)
	}
}

func TestSelectEmptyDomainWins(t *testing.T) {
	c := cfg()
	h := heroSpec(128, 1)
	wls := wlMap(victimWL("v", 100, heroCQ, 2, time.Minute))

	s := snap(
		domain("b1", 256, 0, "v"),
		domain("b2", 128, 0), // empty: zero cost
	)
	plan, reason := SelectDomains(s, h, wls, c, now)
	if plan == nil || len(plan.DomainIDs) != 1 || plan.DomainIDs[0] != "b2" {
		t.Errorf("selected %v (%s), want [b2]", plan.DomainIDs, reason)
	}
	if plan.TotalCost != 0 {
		t.Errorf("TotalCost = %v, want 0", plan.TotalCost)
	}
}

func TestSelectDeterministicTieBreak(t *testing.T) {
	c := cfg()
	h := heroSpec(128, 1)
	s := snap(
		domain("b2", 128, 0),
		domain("b1", 128, 0),
	)
	for range 5 {
		plan, reason := SelectDomains(s, h, nil, c, now)
		if plan == nil || plan.DomainIDs[0] != "b1" {
			t.Fatalf("tie-break not lexicographic: %v (%s)", plan.DomainIDs, reason)
		}
	}
}

func TestSelectVictimPriorityBlocks(t *testing.T) {
	c := cfg()
	h := heroSpec(128, 1)
	wls := wlMap(victimWL("equal-prio", 1000, otherCQ, 2, time.Minute))

	s := snap(domain("b1", 256, 0, "equal-prio"))
	plan, reason := SelectDomains(s, h, wls, c, now)
	if plan != nil || reason != NoFeasibleDomains {
		t.Errorf("outcome = %s, want NoFeasibleDomains (victim priority >= hero)", reason)
	}
}

func TestSelectNonReclaimableShrinksCapacity(t *testing.T) {
	c := cfg()
	h := heroSpec(128, 1)
	// 160 allocatable - 64 non-reclaimable = 96 < 128: infeasible.
	s := snap(domain("b1", 160, 64))
	plan, reason := SelectDomains(s, h, nil, c, now)
	if plan != nil || reason != NoFeasibleDomains {
		t.Errorf("outcome = %s, want NoFeasibleDomains", reason)
	}
}

func TestSelectHeroOccupiedOutcome(t *testing.T) {
	c := cfg()
	h := heroSpec(128, 1)
	occupied := domain("b1", 256, 0)
	occupied.HasOtherHero = true

	// Only capacity-feasible domain hosts another hero.
	plan, reason := SelectDomains(snap(occupied, domain("b2", 64, 0)), h, nil, c, now)
	if plan != nil || reason != AllFeasibleHeroOccupied {
		t.Errorf("outcome = %s, want AllFeasibleHeroOccupied", reason)
	}

	// A hero-free feasible domain exists: hero-occupied one is skipped.
	plan, reason = SelectDomains(snap(occupied, domain("b3", 128, 0)), h, nil, c, now)
	if plan == nil || plan.DomainIDs[0] != "b3" {
		t.Errorf("selected %v (%s), want [b3]", plan.DomainIDs, reason)
	}

	// Regression (found by e2e): the occupying hero's own RUNNING pods sit
	// in the domain as victims at the hero's own priority. The occupied
	// check must fire before the victim-priority filter, or the outcome
	// degrades to NoFeasibleDomains and the waiting hero stops watching
	// for the occupier to finish.
	occupiedWithPods := domain("b1", 256, 0, "occupier")
	occupiedWithPods.HasOtherHero = true
	wls := wlMap(victimWL("occupier", h.Priority, otherCQ, 128, time.Hour))
	plan, reason = SelectDomains(snap(occupiedWithPods, domain("b2", 64, 0)), h, wls, c, now)
	if plan != nil || reason != AllFeasibleHeroOccupied {
		t.Errorf("outcome = %s, want AllFeasibleHeroOccupied when occupier's own pods are the victims", reason)
	}
}

func TestSelectSliceMultiDomain(t *testing.T) {
	c := cfg()
	// Slice-only hero: 3 slices of 32 GPUs.
	h := heroSpec(32, 3)
	wls := wlMap(
		victimWL("r1-wl", 100, heroCQ, 2, time.Minute),
		victimWL("r2-wl", 100, heroCQ, 2, time.Minute),
		victimWL("r3-wl", 900, otherCQ, 200, 40*time.Hour),
		victimWL("r4-wl", 100, heroCQ, 2, time.Minute),
	)

	s := snap(
		domain("r1", 32, 0, "r1-wl"), // 1 chunk, cheap
		domain("r2", 32, 0, "r2-wl"), // 1 chunk, cheap
		domain("r3", 32, 0, "r3-wl"), // 1 chunk, expensive
		domain("r4", 32, 0, "r4-wl"), // 1 chunk, cheap
		domain("r5", 24, 0),          // fragment: 0 chunks, excluded
	)

	plan, reason := SelectDomains(s, h, wls, c, now)
	if plan == nil {
		t.Fatalf("outcome = %s", reason)
	}
	if len(plan.DomainIDs) != 3 {
		t.Fatalf("selected %v, want 3 racks", plan.DomainIDs)
	}
	for _, id := range plan.DomainIDs {
		if id == "r3" {
			t.Error("expensive r3 selected over cheap alternatives")
		}
		if id == "r5" {
			t.Error("fragment r5 selected")
		}
	}
	if got := len(plan.Nodes); got != 6 {
		t.Errorf("nodes = %d, want 6 (2 per rack)", got)
	}
}

func TestSelectMultiChunksDomainPreferred(t *testing.T) {
	c := cfg()
	h := heroSpec(32, 3)
	wls := wlMap(
		victimWL("big-wl", 100, heroCQ, 2, time.Minute),
		victimWL("a-wl", 100, heroCQ, 2, time.Minute),
		victimWL("b-wl", 100, heroCQ, 2, time.Minute),
		victimWL("c-wl", 100, heroCQ, 2, time.Minute),
	)
	// One 96-GPU rack (3 chunks, one victim) vs three 32-GPU racks
	// (1 chunk, one victim each): the big rack's cost-per-chunk is
	// a third, so it wins and one workload is evicted instead of three.
	s := snap(
		domain(bigDomain, 96, 0, "big-wl"),
		domain("r1", 32, 0, "a-wl"),
		domain("r2", 32, 0, "b-wl"),
		domain("r3", 32, 0, "c-wl"),
	)
	plan, reason := SelectDomains(s, h, wls, c, now)
	if plan == nil || len(plan.DomainIDs) != 1 || plan.DomainIDs[0] != bigDomain {
		t.Errorf("selected %v (%s), want [big]", plan.DomainIDs, reason)
	}
}

func TestSelectInsufficientChunks(t *testing.T) {
	c := cfg()
	h := heroSpec(32, 3)
	s := snap(domain("r1", 32, 0), domain("r2", 32, 0)) // only 2 of 3
	plan, reason := SelectDomains(s, h, nil, c, now)
	if plan != nil || reason != NoFeasibleDomains {
		t.Errorf("outcome = %s, want NoFeasibleDomains", reason)
	}
}

func TestSelectDedupesSpanningWorkload(t *testing.T) {
	c := cfg()
	h := heroSpec(32, 2)
	wls := wlMap(victimWL("spanning", 100, heroCQ, 8, time.Minute))

	// Same workload has pods in both racks; drain of both evicts it once.
	s := snap(
		domain("r1", 32, 0, "spanning"),
		domain("r2", 32, 0, "spanning"),
	)
	plan, reason := SelectDomains(s, h, wls, c, now)
	if plan == nil {
		t.Fatalf("outcome = %s", reason)
	}
	if len(plan.Victims) != 1 {
		t.Errorf("victims = %d, want 1 (deduplicated)", len(plan.Victims))
	}
	single := Cost(plan.Victims[0], h, c)
	if math.Abs(plan.TotalCost-single) > 1e-9 {
		t.Errorf("TotalCost = %v, want %v (counted once)", plan.TotalCost, single)
	}
}

func TestSelectGreedyFitPacksOneRack(t *testing.T) {
	c := cfg()
	// Three rack-sized podsets (3 chunks of 32).
	h := heroSpec(32, 3)
	wls := wlMap(
		victimWL("big-wl", 100, heroCQ, 2, time.Minute),
		victimWL("small-wl", 100, heroCQ, 2, time.Minute),
	)
	// One 96-GPU rack fits all three chunks: taint exactly that rack.
	s := snap(
		domain(bigDomain, 96, 0, "big-wl"),
		domain("r1", 32, 0, "small-wl"),
	)
	plan, reason := SelectDomains(s, h, wls, c, now)
	if plan == nil || len(plan.DomainIDs) != 1 || plan.DomainIDs[0] != bigDomain {
		t.Errorf("selected %v (%s), want [big] only", plan.DomainIDs, reason)
	}

	// No rack fits more than one chunk: spill across three racks.
	s = snap(
		domain("r1", 32, 0),
		domain("r2", 32, 0),
		domain("r3", 32, 0),
		domain("r4", 24, 0), // fragment
	)
	plan, reason = SelectDomains(s, h, nil, c, now)
	if plan == nil || len(plan.DomainIDs) != 3 {
		t.Fatalf("selected %v (%s), want 3 racks", plan.DomainIDs, reason)
	}
	for _, id := range plan.DomainIDs {
		if id == "r4" {
			t.Error("fragment r4 selected")
		}
	}
}

func TestSelectMixedChunkSizesLargestFirst(t *testing.T) {
	c := cfg()
	// Ungrouped leader (1 GPU) + workers (128 GPUs): two chunk sizes.
	h := heroWithDemand(
		pkghero.Chunk{Size: *resource.NewQuantity(1, resource.DecimalSI), Count: 1},
		pkghero.Chunk{Size: *resource.NewQuantity(128, resource.DecimalSI), Count: 1},
	)
	// Only bigDomain fits the 128 chunk. The 1-GPU chunk must not claim it
	// first (largest-first ordering) and packs into big's leftover.
	s := snap(
		domain(bigDomain, 129, 0),
		domain("tiny", 8, 0),
	)
	plan, reason := SelectDomains(s, h, nil, c, now)
	if plan == nil {
		t.Fatalf("outcome = %s", reason)
	}
	if len(plan.DomainIDs) != 1 || plan.DomainIDs[0] != bigDomain {
		t.Errorf("selected %v, want [big] with both chunks packed", plan.DomainIDs)
	}
}

func TestSelectSameGroupConstraint(t *testing.T) {
	c := cfg()
	// 2 chunks of 32: rack-level drain for a hero whose podset `required`
	// is the block level, so the racks must share a block.
	h := heroSpec(32, 2)
	wls := wlMap(
		victimWL("cheap-1", 100, heroCQ, 2, time.Minute),
		victimWL("cheap-2", 100, heroCQ, 2, time.Minute),
		victimWL("pricey", 900, otherCQ, 200, 40*time.Hour),
	)

	// Cheapest pair of racks straddles two blocks — must NOT be combined.
	// The only same-block covering pair is in block-2 (one cheap, one
	// expensive rack).
	s := snap(
		domainIn("block-1", "r1", 32, 0, "cheap-1"),
		domainIn("block-2", "r2", 32, 0, "cheap-2"),
		domainIn("block-2", "r3", 32, 0, "pricey"),
	)
	plan, reason := SelectDomains(s, h, wls, c, now)
	if plan == nil {
		t.Fatalf("reason = %s", reason)
	}
	if len(plan.DomainIDs) != 2 || plan.DomainIDs[0] == "r1" || plan.DomainIDs[1] == "r1" {
		t.Errorf("selected %v, want [r2 r3] within block-2 (never cross-block)", plan.DomainIDs)
	}

	// No block can cover both chunks: infeasible even though total
	// cross-block capacity would suffice.
	s = snap(
		domainIn("block-1", "r1", 32, 0),
		domainIn("block-2", "r2", 32, 0),
	)
	plan, reason = SelectDomains(s, h, nil, c, now)
	if plan != nil || reason != NoFeasibleDomains {
		t.Errorf("cross-block cover must be infeasible; got plan=%v reason=%s", plan, reason)
	}
}

// A hero with no podset `required` level leaves Group empty on every
// domain, so the whole snapshot level is one group: the drain may combine
// domains that sit under different ancestors. Grouping by the hierarchy
// unconditionally used to report NoFeasibleDomains here even though kueue
// would place the slices happily.
func TestSelectUngroupedPacksAcrossAncestors(t *testing.T) {
	c := cfg()
	// 3 chunks of 32, and no ancestor holds more than two domains.
	h := heroSpec(32, 3)
	s := snap(
		domain("r1", 32, 0),
		domain("r2", 32, 0),
		domain("r3", 32, 0),
	)
	plan, reason := SelectDomains(s, h, nil, c, now)
	if plan == nil {
		t.Fatalf("ungrouped hero must pack across the level; reason = %s", reason)
	}
	if len(plan.DomainIDs) != 3 {
		t.Errorf("selected %v, want all three domains", plan.DomainIDs)
	}
}

func TestSelectCheapestGroupWins(t *testing.T) {
	c := cfg()
	h := heroSpec(32, 2)
	wls := wlMap(
		victimWL("mild", 300, heroCQ, 4, time.Hour),
		victimWL("harsh", 900, otherCQ, 200, 40*time.Hour),
	)
	// Both blocks can cover; block-1's victims cost less.
	s := snap(
		domainIn("block-1", "r1", 32, 0, "mild"),
		domainIn("block-1", "r2", 32, 0),
		domainIn("block-2", "r3", 32, 0, "harsh"),
		domainIn("block-2", "r4", 32, 0),
	)
	plan, reason := SelectDomains(s, h, wls, c, now)
	if plan == nil {
		t.Fatalf("reason = %s", reason)
	}
	for _, id := range plan.DomainIDs {
		if id == "r3" || id == "r4" {
			t.Errorf("selected %v from the pricier block", plan.DomainIDs)
		}
	}
}

// Invariant: a chunk that fits an already-selected domain NEVER taints a
// second domain, regardless of how attractive other domains look.
func TestSelectNeverTaintsExtraDomainWhenChunkFits(t *testing.T) {
	c := cfg()
	// Two podset chunks; both fit the first-picked domain.
	h := heroSpec(32, 2)
	wls := wlMap(victimWL("v", 100, heroCQ, 2, time.Minute))
	s := snap(
		domain("d1", 64, 0, "v"), // fits both chunks, has a victim
		domain("d2", 640, 0),     // empty and enormous — still must not be tainted
		domain("d3", 64, 0),      // empty twin — still must not be tainted
	)
	plan, reason := SelectDomains(s, h, wls, c, now)
	if plan == nil {
		t.Fatalf("outcome = %s", reason)
	}
	if len(plan.DomainIDs) != 1 {
		t.Fatalf("tainted %v, want exactly one domain", plan.DomainIDs)
	}
}
