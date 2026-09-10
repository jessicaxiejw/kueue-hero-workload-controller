// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package selection

import (
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/config"
	pkghero "github.com/coreweave/kueue-hero-workload-controller/pkg/hero"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/snapshot"
)

var now = time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)

const (
	heroCQ  = "hero-cq"
	otherCQ = "other-cq"
)

func cfg() *config.Config {
	c := config.Default()
	return &c
}

func heroSpec(chunkGPUs int64, chunks int) HeroSpec {
	return heroWithDemand(pkghero.Chunk{
		Size:  *resource.NewQuantity(chunkGPUs, resource.DecimalSI),
		Count: chunks,
	})
}

func heroWithDemand(demand ...pkghero.Chunk) HeroSpec {
	return HeroSpec{
		Key:          types.NamespacedName{Namespace: "team-a", Name: "hero"},
		Priority:     1000,
		PodCount:     16,
		ClusterQueue: heroCQ,
		Demand:       demand,
	}
}

// domain builds a snapshot.Domain with the given usable capacity and one
// victim pod per named workload.
func domain(id string, allocatable, nonReclaimable int64, victimWLs ...string) *snapshot.Domain {
	return domainIn("", id, allocatable, nonReclaimable, victimWLs...)
}

// domainIn builds a domain inside the given grouping domain.
func domainIn(group, id string, allocatable, nonReclaimable int64, victimWLs ...string) *snapshot.Domain {
	d := &snapshot.Domain{
		ID:                id,
		Group:             group,
		Nodes:             []string{id + "-n1", id + "-n2"},
		AllocatableGPU:    *resource.NewQuantity(allocatable, resource.DecimalSI),
		NonReclaimableGPU: *resource.NewQuantity(nonReclaimable, resource.DecimalSI),
	}
	for _, wl := range victimWLs {
		d.Victims = append(d.Victims, snapshot.Victim{
			Pod:      types.NamespacedName{Namespace: "tenant", Name: wl + "-pod"},
			Node:     id + "-n1",
			Workload: types.NamespacedName{Namespace: "tenant", Name: wl},
		})
	}
	return d
}

func snap(domains ...*snapshot.Domain) *snapshot.Snapshot {
	s := &snapshot.Snapshot{Level: "block", Domains: map[string]*snapshot.Domain{}}
	for _, d := range domains {
		s.Domains[d.ID] = d
	}
	return s
}

// victimWL builds a Workload for the join: priority, admitted CQ, admitted
// pod count, admitted at now-age.
func victimWL(name string, priority int32, cq string, pods int32, age time.Duration) *kueue.Workload {
	admission := utiltesting.MakeAdmission(kueue.ClusterQueueReference(cq)).
		PodSets(kueue.PodSetAssignment{Name: "main", Count: &pods}).Obj()
	return utiltesting.MakeWorkload(name, "tenant").
		Priority(priority).
		ReserveQuotaAt(admission, now.Add(-age)).
		AdmittedAt(true, now.Add(-age)).
		Obj()
}

func wlMap(wls ...*kueue.Workload) map[types.NamespacedName]*kueue.Workload {
	m := map[types.NamespacedName]*kueue.Workload{}
	for _, wl := range wls {
		m[types.NamespacedName{Namespace: wl.Namespace, Name: wl.Name}] = wl
	}
	return m
}
