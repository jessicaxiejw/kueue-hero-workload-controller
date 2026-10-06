// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package selection

import (
	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
)

// Cost is the design doc's per-victim disruption cost:
//
//	m_rel · (w_p·p̂ + w_n·n̂ + w_r·r̂)
//
// p̂ = victim priority / hero priority (importance, clamped to [0,1] —
// feasibility already guarantees victim < hero), n̂ = victim pods /
// (victim pods + hero pods) (size, saturating), r̂ = running /
// (running + half-life) (age, saturating). m_rel = CrossCQMultiplier for
// victims from another ClusterQueue, 1 for the hero's own. Weights rank
// victims against each other; they never decide WHETHER eviction happens.
func Cost(v VictimWorkload, hero HeroSpec, cfg *config.Config) float64 {
	pHat := 0.0
	if hero.Priority > 0 && v.Priority > 0 {
		pHat = min(float64(v.Priority)/float64(hero.Priority), 1)
	}
	nHat := 0.0
	if v.PodCount > 0 {
		nHat = float64(v.PodCount) / float64(v.PodCount+hero.PodCount)
	}
	rHat := 0.0
	if v.RunningFor > 0 {
		half := cfg.RuntimeHalfLife.Seconds()
		run := v.RunningFor.Seconds()
		rHat = run / (run + half)
	}

	mRel := 1.0
	if v.ClusterQueue != hero.ClusterQueue {
		mRel = cfg.CrossCQMultiplier
	}
	if v.Cohort != hero.Cohort && v.Cohort != "" && hero.Cohort != "" {
		mRel = cfg.CrossCohortMultiplier
	}

	return mRel * (cfg.Weights.Priority*pHat + cfg.Weights.PodCount*nHat + cfg.Weights.Runtime*rHat)
}
