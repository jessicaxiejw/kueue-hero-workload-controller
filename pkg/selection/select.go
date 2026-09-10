// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

// Package selection implements design doc Phase 1: filter the snapshot's
// domains for feasibility, score each by the disruption its drain would
// cause, and pick the cheapest set of domains covering the hero's demand.
// Pure functions, no cluster access.
package selection

import (
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/hero"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/snapshot"
)

// HeroSpec is everything the selector needs to know about the hero;
// derived by the reconciler from the hero Workload (pkg/hero helpers).
type HeroSpec struct {
	Key      types.NamespacedName
	Priority int32
	// PodCount is the hero's spec pod count (full ask) — the n̂ normalizer.
	PodCount int32
	// ClusterQueue the hero is submitted through; victims from other CQs
	// cost CrossCQMultiplier more to evict.
	ClusterQueue string
	// Demand is the list of contiguous-capacity chunks the drained domain
	// set must supply (hero.Demand).
	Demand []hero.Chunk
}

// InfeasibleReason says why no drain plan was possible.
type InfeasibleReason string

const (
	// NoFeasibleDomains: no domain set can cover the demand even after
	// eviction — capacity or victim-priority is the blocker; draining
	// cannot help.
	NoFeasibleDomains InfeasibleReason = "NoFeasibleDomains"
	// AllFeasibleHeroOccupied: at least one domain fails ONLY because
	// another hero occupies it, and no hero-free set covers the demand.
	// Per the design doc, the drain waits for that hero instead of
	// stacking heroes into one domain.
	AllFeasibleHeroOccupied InfeasibleReason = "AllFeasibleHeroOccupied"
)

// DrainPlan is the drain to execute: which domains to taint, every node in
// them, and the workloads that will be evicted.
type DrainPlan struct {
	// DomainIDs to drain, in selection order.
	DomainIDs []string
	// Nodes across all selected domains — the taint targets.
	Nodes []string
	// TotalCost is the summed disruption cost, workloads deduplicated
	// (a workload spanning two selected domains is evicted once).
	TotalCost float64
	// Victims are the workloads the drain will suspend, deduplicated.
	Victims []VictimWorkload
}

// candidate is a domain that passed feasibility, with its remaining
// reclaimable capacity tracked as chunks are packed into it.
type candidate struct {
	domain    *snapshot.Domain
	victims   []VictimWorkload
	cost      float64
	remaining resource.Quantity
	selected  bool
}

// reclaimable is the capacity a full drain of the domain frees for the
// hero: allocatable minus what cannot move.
func reclaimable(d *snapshot.Domain) resource.Quantity {
	r := d.AllocatableGPU.DeepCopy()
	r.Sub(d.NonReclaimableGPU)
	if r.Sign() < 0 {
		return resource.Quantity{}
	}
	return r
}

// victimsBelowHero applies the design doc's victim-priority check: a domain
// containing any workload at or above the hero's priority is not drainable.
func victimsBelowHero(victims []VictimWorkload, heroPriority int32) bool {
	for i := range victims {
		if victims[i].Priority >= heroPriority {
			return false
		}
	}
	return true
}

// SelectDomains picks the min-cost feasible domain set covering the hero's
// demand, packing chunks into as few domains as possible. It returns
// exactly one of: a plan to execute (non-nil), or the reason none is
// possible.
//
// Selected domains must share a snapshot.Domain.Group — the ancestor the
// hero's podset `required` topology forces its slices into. Heroes that
// declare no `required` level have an empty Group on every domain, which
// collapses to a single group and packs freely across the whole level;
// that matches kueue, which spreads unconstrained slices wherever they
// fit. Greedy fit runs independently per group; the cheapest covering
// group wins. Pure planning — the returned plan is executed later (taint
// everything first, then evict), nothing changes in the cluster while
// this runs.
func SelectDomains(
	s *snapshot.Snapshot,
	heroSpec HeroSpec,
	workloads map[types.NamespacedName]*kueue.Workload,
	cfg *config.Config,
	now time.Time,
) (*DrainPlan, InfeasibleReason) {
	byGroup := map[string][]*candidate{}
	heroBlocked := false

	ids := make([]string, 0, len(s.Domains))
	for id := range s.Domains {
		ids = append(ids, id)
	}
	slices.Sort(ids) // deterministic iteration

	for _, id := range ids {
		d := s.Domains[id]
		// Hero-occupied domains are excluded FIRST: the occupying hero's own
		// running pods would otherwise trip the victim-priority filter below
		// (a hero is never below another hero's priority) and misreport the
		// wait-for-that-hero situation as NoFeasibleDomains.
		if d.HasOtherHero {
			heroBlocked = true
			continue
		}
		victims := GroupVictims(d, workloads, now)
		rec := reclaimable(d)
		if rec.Sign() <= 0 || !victimsBelowHero(victims, heroSpec.Priority) {
			continue
		}
		cost := 0.0
		for i := range victims {
			cost += Cost(victims[i], heroSpec, cfg)
		}
		byGroup[d.Group] = append(byGroup[d.Group], &candidate{
			domain:    d,
			victims:   victims,
			cost:      cost,
			remaining: rec,
		})
	}

	// Expand demand into units, largest first, so big chunks claim the
	// scarce large domains before small chunks fragment them.
	var units []resource.Quantity
	for _, c := range heroSpec.Demand {
		for range c.Count {
			units = append(units, c.Size)
		}
	}
	slices.SortFunc(units, func(a, b resource.Quantity) int { return b.Cmp(a) })
	if len(units) == 0 {
		return nil, NoFeasibleDomains // empty demand; caller gates on this
	}

	groups := make([]string, 0, len(byGroup))
	for group := range byGroup {
		groups = append(groups, group)
	}
	slices.Sort(groups) // deterministic iteration

	var best *DrainPlan
	for _, group := range groups {
		plan := packWithinGroup(byGroup[group], units, heroSpec, cfg)
		if plan == nil {
			continue
		}
		if best == nil || plan.TotalCost < best.TotalCost {
			best = plan
		}
	}
	if best == nil {
		if heroBlocked {
			return nil, AllFeasibleHeroOccupied
		}
		return nil, NoFeasibleDomains
	}
	return best, ""
}

// packWithinGroup greedily fits the demand units into one group's
// candidates; nil when the group cannot cover the demand.
func packWithinGroup(cands []*candidate, units []resource.Quantity, heroSpec HeroSpec, cfg *config.Config) *DrainPlan {
	for _, c := range cands { // reset packing state (groups are retried)
		c.remaining = reclaimable(c.domain)
		c.selected = false
	}
	for _, unit := range units {
		var best *candidate
		for _, c := range cands {
			if c.remaining.Cmp(unit) < 0 {
				continue
			}
			if best == nil || packBetter(c, best) {
				best = c
			}
		}
		if best == nil {
			return nil
		}
		best.remaining.Sub(unit)
		best.selected = true
	}

	plan := &DrainPlan{}
	seenWL := map[types.NamespacedName]bool{}
	for _, c := range cands {
		if !c.selected {
			continue
		}
		plan.DomainIDs = append(plan.DomainIDs, c.domain.ID)
		plan.Nodes = append(plan.Nodes, c.domain.Nodes...)
		for j := range c.victims {
			if seenWL[c.victims[j].Key] {
				continue // spans domains; evicted (and costed) once
			}
			seenWL[c.victims[j].Key] = true
			plan.Victims = append(plan.Victims, c.victims[j])
			plan.TotalCost += Cost(c.victims[j], heroSpec, cfg)
		}
	}
	return plan
}

// packBetter reports whether candidate a is a better home for the current
// unit than b. Selection is pure planning — nothing is tainted or evicted
// until the whole Decision executes — so "selected" means "this plan
// already includes draining that domain" and its cost is already counted.
// Order: lowest marginal cost first (a domain already in the plan adds
// zero), then already-in-plan over untouched — together these guarantee a
// chunk that fits an already-selected domain never taints an extra one —
// then the roomiest domain so later chunks can pack into it (units are
// placed largest-first, so large chunks are never crowded out by small
// ones), then ID for determinism.
func packBetter(a, b *candidate) bool {
	am, bm := a.marginalCost(), b.marginalCost()
	if am != bm {
		return am < bm
	}
	if a.selected != b.selected {
		return a.selected // fewer tainted domains, even at zero cost
	}
	if c := a.remaining.Cmp(b.remaining); c != 0 {
		return c > 0 // roomier wins
	}
	return strings.Compare(a.domain.ID, b.domain.ID) < 0
}

func (c *candidate) marginalCost() float64 {
	if c.selected {
		return 0
	}
	return c.cost
}
