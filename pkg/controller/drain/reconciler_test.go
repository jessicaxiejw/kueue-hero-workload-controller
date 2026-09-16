// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package drainctrl

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
	testingpod "sigs.k8s.io/kueue/pkg/util/testingjobs/pod"

	"github.com/coreweave/kueue-hero-workload-controller/pkg/metrics"
	"github.com/coreweave/kueue-hero-workload-controller/pkg/taint"
	"github.com/coreweave/kueue-hero-workload-controller/test/utils/fakekueue"
)

const (
	nodeB1N1   = "b1-n1"
	nodeB1N2   = "b1-n2"
	levelBlock = "cloud.provider.com/topology-block"
	gpuRes     = corev1.ResourceName("nvidia.com/gpu")
	heroCQName = "hero-cq"
	victimCQ   = "tenant-cq"
)

// eachSpecNamespace gives every spec its own namespace; cluster-scoped
// leftovers (nodes, CQs) are deleted per spec in AfterEach.
var (
	ns      *corev1.Namespace
	specIdx int
)

var _ = BeforeEach(func() {
	specIdx++
	ns = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("spec-%d", specIdx)}}
	Expect(k8sClient.Create(ctx, ns)).To(Succeed())
	testCfg.DryRun = false
})

var _ = AfterEach(func() {
	// envtest has no namespace GC: deleting the namespace does not cascade,
	// and lingering stuck heroes would poison later specs' NextHero gate.
	Expect(k8sClient.DeleteAllOf(ctx, &kueue.Workload{}, client.InNamespace(ns.Name))).To(Succeed())
	// Pods need grace 0 AND a wait: envtest has no kubelet to finish a
	// graceful deletion, so default-grace pods linger as Terminating
	// forever and haunt later specs (node names are reused).
	zero := int64(0)
	Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{}, client.InNamespace(ns.Name),
		client.GracePeriodSeconds(zero))).To(Succeed())
	Eventually(func(g Gomega) {
		pods := &corev1.PodList{}
		g.Expect(k8sClient.List(ctx, pods, client.InNamespace(ns.Name))).To(Succeed())
		g.Expect(pods.Items).To(BeEmpty())
	}).Should(Succeed())
	Expect(k8sClient.DeleteAllOf(ctx, &kueue.LocalQueue{}, client.InNamespace(ns.Name))).To(Succeed())
	Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
	Expect(k8sClient.DeleteAllOf(ctx, &kueue.ClusterQueue{})).To(Succeed())
	Expect(k8sClient.DeleteAllOf(ctx, &kueue.ResourceFlavor{})).To(Succeed())
	Expect(k8sClient.DeleteAllOf(ctx, &kueue.Topology{})).To(Succeed())
	Expect(k8sClient.Delete(ctx, ns)).To(Succeed())
})

// setupTopologyFixtures creates Topology, ResourceFlavor, hero CQ + LQ,
// victim CQ, and two blocks of two 8-GPU nodes each.
func setupTopologyFixtures(heroQuota string) {
	topo := &kueue.Topology{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: kueue.TopologySpec{Levels: []kueue.TopologyLevel{
			{NodeLabel: levelBlock},
			{NodeLabel: corev1.LabelHostname},
		}},
	}
	Expect(k8sClient.Create(ctx, topo)).To(Succeed())

	rf := utiltesting.MakeResourceFlavor("gpu-flavor").
		NodeLabel("node-group", "tas"). // kueue CRD: topology requires >=1 nodeLabel
		TopologyName("default").Obj()
	Expect(k8sClient.Create(ctx, rf)).To(Succeed())

	heroCQ := utiltesting.MakeClusterQueue(heroCQName).
		Label(testCfg.HeroCQLabelKey, "true").
		ResourceGroup(*utiltesting.MakeFlavorQuotas("gpu-flavor").
			Resource(gpuRes, heroQuota).Obj()).
		Obj()
	Expect(k8sClient.Create(ctx, heroCQ)).To(Succeed())

	tenantCQ := utiltesting.MakeClusterQueue(victimCQ).
		ResourceGroup(*utiltesting.MakeFlavorQuotas("gpu-flavor").
			Resource(gpuRes, "256").Obj()).
		Obj()
	Expect(k8sClient.Create(ctx, tenantCQ)).To(Succeed())

	lq := utiltesting.MakeLocalQueue("hero-queue", ns.Name).ClusterQueue(heroCQName).Obj()
	Expect(k8sClient.Create(ctx, lq)).To(Succeed())

	for _, n := range []struct{ name, block string }{
		{nodeB1N1, "b1"}, {nodeB1N2, "b1"},
		{"b2-n1", "b2"}, {"b2-n2", "b2"},
	} {
		node := testingnode.MakeNode(n.name).
			Label(levelBlock, n.block).
			Label(corev1.LabelHostname, n.name).
			StatusAllocatable(corev1.ResourceList{gpuRes: resource.MustParse("8")}).
			Ready().Obj()
		createNodeWithStatus(node)
	}
}

// createNodeWithStatus creates a node and then writes its status: status
// is a subresource, so Create alone silently drops allocatable/conditions.
func createNodeWithStatus(node *corev1.Node) {
	want := node.Status
	Expect(k8sClient.Create(ctx, node)).To(Succeed())
	Eventually(func(g Gomega) {
		created := &corev1.Node{}
		g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: node.Name}, created)).To(Succeed())
		created.Status = want
		g.Expect(k8sClient.Status().Update(ctx, created)).To(Succeed())
	}).Should(Succeed())
	// The envtest apiserver's TaintNodesByCondition admission taints new
	// nodes node.kubernetes.io/not-ready:NoSchedule, and no node-lifecycle
	// controller runs here to lift it once Ready is set — strip it, or
	// every node counts as unusable.
	Eventually(func(g Gomega) {
		created := &corev1.Node{}
		g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: node.Name}, created)).To(Succeed())
		created.Spec.Taints = nil
		g.Expect(k8sClient.Update(ctx, created)).To(Succeed())
	}).Should(Succeed())
}

// createHero creates a stuck hero workload requesting pods x 8 GPUs, with
// the slice pair (the hero contract): one slice spanning all pods, so the
// demand is a single block-sized chunk.
func createHero(name string, pods int, priority int32) *kueue.Workload {
	const gpusPerPod = 8
	wl := utiltesting.MakeWorkload(name, ns.Name).
		Queue("hero-queue").
		WorkloadPriorityClassRef(testCfg.HeroPriorityClassName).
		Priority(priority).
		PodSets(*utiltesting.MakePodSet("main", pods).
			SliceRequiredTopologyRequest(levelBlock).
			SliceSizeTopologyRequest(int32(pods)).
			Request(gpuRes, fmt.Sprintf("%d", gpusPerPod)).
			Toleration(corev1.Toleration{Key: testCfg.TaintKey, Operator: corev1.TolerationOpExists}).
			Obj()).
		Obj()
	Expect(k8sClient.Create(ctx, wl)).To(Succeed())
	Expect(fakekueue.MarkStuck(ctx, k8sClient, wl, levelBlock, pods)).To(Succeed())
	return wl
}

// createUnstuckHero is createHero minus the stuck condition: a workload
// that is a hero by every marker (CQ label, priority class, toleration,
// slice topology) but that kueue has not declared unable to fit.
func createUnstuckHero(name string, pods int, priority int32) *kueue.Workload {
	const gpusPerPod = 8
	wl := utiltesting.MakeWorkload(name, ns.Name).
		Queue("hero-queue").
		WorkloadPriorityClassRef(testCfg.HeroPriorityClassName).
		Priority(priority).
		PodSets(*utiltesting.MakePodSet("main", pods).
			SliceRequiredTopologyRequest(levelBlock).
			SliceSizeTopologyRequest(int32(pods)).
			Request(gpuRes, fmt.Sprintf("%d", gpusPerPod)).
			Toleration(corev1.Toleration{Key: testCfg.TaintKey, Operator: corev1.TolerationOpExists}).
			Obj()).
		Obj()
	Expect(k8sClient.Create(ctx, wl)).To(Succeed())
	return wl
}

// createVictim creates an admitted victim workload with running pods on
// the given nodes (one pod per entry, 8 GPUs each).
func createVictim(name string, priority int32, nodes ...string) *kueue.Workload {
	wl := utiltesting.MakeWorkload(name, ns.Name).
		Priority(priority).
		PodSets(*utiltesting.MakePodSet("main", len(nodes)).
			Request(gpuRes, "8").Obj()).
		Obj()
	Expect(k8sClient.Create(ctx, wl)).To(Succeed())

	perHost := map[string]int32{}
	for _, n := range nodes {
		perHost[n]++
	}
	Expect(fakekueue.Admit(ctx, k8sClient, wl, victimCQ, perHost)).To(Succeed())

	for i, node := range nodes {
		pod := testingpod.MakePod(fmt.Sprintf("%s-%d", name, i), ns.Name).
			NodeName(node).
			RequestAndLimit(gpuRes, "8").
			Annotation(kueue.WorkloadAnnotation, name).
			StatusPhase(corev1.PodRunning).
			Obj()
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	}
	return wl
}

func taintsOwnedBy(owner types.NamespacedName) func() []string {
	return func() []string {
		nodes := &corev1.NodeList{}
		Expect(k8sClient.List(ctx, nodes)).To(Succeed())
		var out []string
		for i := range nodes.Items {
			if got, ok := taint.Owner(&nodes.Items[i], testCfg.TaintKey); ok && got == owner {
				out = append(out, nodes.Items[i].Name)
			}
		}
		return out
	}
}

// touchNode patches a throwaway label on a tainted node so the drain
// controller reconciles its owner now instead of at the next 15s requeue —
// pod deletions are not watched, so emptying a domain wakes nobody.
func touchNode(name string) {
	GinkgoHelper()
	node := &corev1.Node{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, node)).To(Succeed())
	patch := client.MergeFrom(node.DeepCopy())
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	node.Labels["test/touch"] = fmt.Sprintf("%d", time.Now().UnixNano())
	Expect(k8sClient.Patch(ctx, node, patch)).To(Succeed())
}

func getWL(key types.NamespacedName) func() *kueue.Workload {
	return func() *kueue.Workload {
		wl := &kueue.Workload{}
		Expect(k8sClient.Get(ctx, key, wl)).To(Succeed())
		return wl
	}
}

func keyOf(wl *kueue.Workload) types.NamespacedName {
	return types.NamespacedName{Namespace: wl.Namespace, Name: wl.Name}
}

var _ = Describe("drain controller", func() {
	It("drains the cheaper block and cycles victims through suspend/unsuspend", func() {
		setupTopologyFixtures("256")
		startedBefore := testutil.ToFloat64(metrics.DrainsStarted)
		plannedBefore := testutil.ToFloat64(metrics.SelectionOutcomes.WithLabelValues(metrics.SelectionPlanned))
		suspendedBefore := testutil.ToFloat64(metrics.VictimsSuspended)
		reactivatedBefore := testutil.ToFloat64(metrics.VictimsReactivated)
		// b1: one small young victim. b2: big old cross-CQ victim.
		small := createVictim("small-victim", 100, nodeB1N1, nodeB1N2)
		createVictim("big-victim", 900, "b2-n1", "b2-n2")

		heroWL := createHero("hero-1", 2, 1000)
		heroKey := keyOf(heroWL)

		By("tainting both b1 nodes for the hero")
		Eventually(taintsOwnedBy(heroKey)).Should(ConsistOf(nodeB1N1, nodeB1N2))

		By("counting the drain start and the planned selection exactly once")
		Eventually(func() float64 { return testutil.ToFloat64(metrics.DrainsStarted) }).
			Should(Equal(startedBefore + 1))
		Eventually(func() float64 {
			return testutil.ToFloat64(metrics.SelectionOutcomes.WithLabelValues(metrics.SelectionPlanned))
		}).Should(BeNumerically(">=", plannedBefore+1))

		By("suspending the b1 victim with our ownership annotation")
		Eventually(func() bool {
			v := getWL(keyOf(small))()
			return v.Spec.Active != nil && !*v.Spec.Active &&
				v.Annotations[DeactivatedForAnnotation] == heroKey.Namespace+"/"+heroKey.Name
		}).Should(BeTrue())

		By("leaving the b2 victim untouched")
		Consistently(func() bool {
			v := getWL(types.NamespacedName{Namespace: ns.Name, Name: "big-victim"})()
			return v.Spec.Active == nil || *v.Spec.Active
		}, "2s").Should(BeTrue())

		By("recording the suspension event on the victim")
		Eventually(eventsFor(small)).Should(ContainElement(EventSuspendedForHero))

		By("reactivating the victim after kueue evicts it")
		Eventually(func() (int, error) { return fakekueue.EvictDeactivated(ctx, k8sClient) }).
			Should(BeNumerically(">", 0))
		Eventually(func() bool {
			v := getWL(keyOf(small))()
			_, marked := v.Annotations[DeactivatedForAnnotation]
			return v.Spec.Active != nil && *v.Spec.Active && !marked
		}).Should(BeTrue())

		By("recording the reactivation event on the victim")
		Eventually(eventsFor(small)).Should(ContainElement(EventReactivatedByDrain))

		By("counting the victim's suspend and reactivate")
		Expect(testutil.ToFloat64(metrics.VictimsSuspended)).To(Equal(suspendedBefore + 1))
		Expect(testutil.ToFloat64(metrics.VictimsReactivated)).To(Equal(reactivatedBefore + 1))

		By("keeping the taints up (untainting is the janitor's job)")
		Consistently(taintsOwnedBy(heroKey), "2s").Should(ConsistOf(nodeB1N1, nodeB1N2))
	})

	It("nudges kueue via a drained node once the domain is empty of victims", func() {
		// Kueue 0.16.9 neither notices victim pods finishing termination
		// nor honors hero Workload updates once it has memorized the
		// hero's shape as NoFit — only a NODE change wipes that memory.
		// The controller must produce one. See taint.NudgeAnnotation.
		setupTopologyFixtures("256")
		small := createVictim("small-victim", 100, nodeB1N1, nodeB1N2)
		createVictim("big-victim", 900, "b2-n1", "b2-n2")
		heroWL := createHero("hero-1", 2, 1000)
		heroKey := keyOf(heroWL)

		nudgeOf := func(nodeName string) string {
			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, node)).To(Succeed())
			return node.Annotations[taint.NudgeAnnotation]
		}

		By("draining b1: taints up, victim suspended")
		Eventually(taintsOwnedBy(heroKey)).Should(ConsistOf(nodeB1N1, nodeB1N2))
		Eventually(func() bool {
			v := getWL(keyOf(small))()
			return v.Spec.Active != nil && !*v.Spec.Active
		}).Should(BeTrue())

		By("no nudge while the victim's pods still occupy the domain")
		Consistently(func() string { return nudgeOf(nodeB1N1) }, "2s").Should(BeEmpty())

		By("kueue evicts the victim; its pods vanish; the domain is empty")
		Eventually(func() (int, error) { return fakekueue.EvictDeactivated(ctx, k8sClient) }).
			Should(BeNumerically(">", 0))

		By("the first drained node gets the scheduling-nudge annotation")
		Eventually(func() string {
			touchNode(nodeB1N2) // wake the reconcile; the nudge target is the min-named node
			return nudgeOf(nodeB1N1)
		}, "10s", "1s").Should(Equal(testNow.UTC().Format(time.RFC3339)))
	})

	It("leaves an unstuck hero and the cluster untouched", func() {
		// The drain trigger is hero-ness AND the stuck signal, never
		// hero-ness alone: a hero kueue has not marked TAS-no-fit must
		// cause no taints, no suspensions, no events — kueue is handling
		// it on its own.
		setupTopologyFixtures("256")
		small := createVictim("small-victim", 100, nodeB1N1, nodeB1N2)
		big := createVictim("big-victim", 900, "b2-n1", "b2-n2")
		hero := createUnstuckHero("content-hero", 2, 1000)

		By("tainting nothing for anyone")
		Consistently(func() []string {
			nodes := &corev1.NodeList{}
			Expect(k8sClient.List(ctx, nodes)).To(Succeed())
			var tainted []string
			for i := range nodes.Items {
				if _, ok := taint.Owner(&nodes.Items[i], testCfg.TaintKey); ok {
					tainted = append(tainted, nodes.Items[i].Name)
				}
			}
			return tainted
		}, "3s").Should(BeEmpty())

		By("leaving every victim active and unmarked")
		for _, victim := range []*kueue.Workload{small, big} {
			v := getWL(keyOf(victim))()
			Expect(v.Spec.Active == nil || *v.Spec.Active).To(BeTrue())
			Expect(v.Annotations).NotTo(HaveKey(DeactivatedForAnnotation))
		}

		By("recording no drain events on the hero")
		Expect(eventsFor(hero)()).To(BeEmpty())
	})

	It("refuses to drain when the hero exceeds nominal quota", func() {
		setupTopologyFixtures("8") // hero wants 16
		heroWL := createHero("hero-overquota", 2, 1000)

		Consistently(taintsOwnedBy(keyOf(heroWL)), "3s").Should(BeEmpty())
		Eventually(eventsFor(heroWL)).Should(ContainElement(EventHeroExceedsQuota))
	})

	It("queues a second hero while a foreign drain is in flight", func() {
		setupTopologyFixtures("256")
		// The foreign drain's owner must be a real, live, hero-classed
		// workload: the janitor tears down abandoned owners, and the drain
		// controller aborts drains owned by non-heroes — both would
		// otherwise rightly dissolve this fixture.
		otherWL := utiltesting.MakeWorkload("hero-other", ns.Name).
			Queue("hero-queue").
			WorkloadPriorityClassRef(testCfg.HeroPriorityClassName).
			Priority(500).
			PodSets(*utiltesting.MakePodSet("main", 1).
				SliceRequiredTopologyRequest(levelBlock).
				SliceSizeTopologyRequest(1).
				Request(gpuRes, "8").
				Toleration(corev1.Toleration{Key: testCfg.TaintKey, Operator: corev1.TolerationOpExists}).
				Obj()).
			Obj()
		Expect(k8sClient.Create(ctx, otherWL)).To(Succeed())
		other := keyOf(otherWL)
		Expect(taint.EnsureTaint(ctx, k8sClient, "b2-n1", testCfg.TaintKey, other, "other-hero-cq", testNow)).To(Succeed())

		heroWL := createHero("hero-queued", 2, 1000)

		Eventually(eventsFor(heroWL)).Should(ContainElement(EventDrainQueued))
		Consistently(taintsOwnedBy(keyOf(heroWL)), "2s").Should(BeEmpty())
	})

	It("keeps a second stuck hero queued behind the first hero's drain", func() {
		setupTopologyFixtures("256")
		// Sequential creation makes true simultaneity impossible: whoever
		// reconciles while alone in the stuck list legitimately wins (the
		// priority ordering itself is unit-tested in pkg/drain). What this
		// spec pins is the serialization: once the first hero's drain is
		// in flight, a second stuck hero must not taint anything.
		winner := createHero("hero-first", 2, 2000)
		Eventually(taintsOwnedBy(keyOf(winner))).ShouldNot(BeEmpty())

		second := createHero("hero-second", 2, 3000) // even higher priority: still waits
		Eventually(eventsFor(second)).Should(ContainElement(EventDrainQueued))
		Consistently(taintsOwnedBy(keyOf(second)), "2s").Should(BeEmpty())
	})

	It("plans without acting in dry-run mode", func() {
		setupTopologyFixtures("256")
		testCfg.DryRun = true
		DeferCleanup(func() { testCfg.DryRun = false })

		// Victims occupy both blocks so a real drain WOULD suspend some of
		// them — dry run must leave every one running and unmarked.
		small := createVictim("small-victim", 100, nodeB1N1, nodeB1N2)
		big := createVictim("big-victim", 900, "b2-n1", "b2-n2")
		heroWL := createHero("hero-dryrun", 2, 1000)

		Eventually(eventsFor(heroWL)).Should(ContainElement(EventDrainPlannedDryRun))
		Consistently(taintsOwnedBy(keyOf(heroWL)), "2s").Should(BeEmpty())
		for _, victim := range []*kueue.Workload{small, big} {
			v := getWL(keyOf(victim))()
			Expect(v.Spec.Active == nil || *v.Spec.Active).To(BeTrue())
			Expect(v.Annotations).NotTo(HaveKey(DeactivatedForAnnotation))
			Expect(eventsFor(victim)()).To(BeEmpty())
		}
	})

	It("emits NoFeasibleDomains when nothing can fit the hero", func() {
		setupTopologyFixtures("256")
		heroWL := createHero("hero-toobig", 8, 1000) // 64 GPUs > any block

		Eventually(eventsFor(heroWL)).Should(ContainElement(EventNoFeasibleDomains))
		Consistently(taintsOwnedBy(keyOf(heroWL)), "2s").Should(BeEmpty())
	})

	It("wakes a hero waiting on hero-occupied domains when the blocker finishes", func() {
		setupTopologyFixtures("256")

		// hero-A: admitted, occupying BOTH blocks (one pod on each).
		// Slice size 1 so a pod per block is consistent with its own
		// topology request; the slice pair itself is what makes it a hero
		// whose domains other heroes must leave alone.
		heroA := utiltesting.MakeWorkload("hero-a", ns.Name).
			Queue("hero-queue").
			WorkloadPriorityClassRef(testCfg.HeroPriorityClassName).
			Priority(1500).
			PodSets(*utiltesting.MakePodSet("main", 2).
				SliceRequiredTopologyRequest(levelBlock).
				SliceSizeTopologyRequest(1).
				Request(gpuRes, "8").
				Toleration(corev1.Toleration{Key: testCfg.TaintKey, Operator: corev1.TolerationOpExists}).
				Obj()).
			Obj()
		Expect(k8sClient.Create(ctx, heroA)).To(Succeed())
		Expect(fakekueue.Admit(ctx, k8sClient, heroA, heroCQName,
			map[string]int32{nodeB1N1: 1, "b2-n1": 1})).To(Succeed())

		// hero-B: stuck; every feasible block hosts hero-A.
		heroB := createHero("hero-b", 2, 1000)

		By("reporting hero-occupied and tainting nothing")
		Eventually(eventsFor(heroB)).Should(ContainElement(EventAllFeasibleHeroOccupied))
		Consistently(taintsOwnedBy(keyOf(heroB)), "2s").Should(BeEmpty())

		By("draining promptly once hero-A finishes")
		Expect(fakekueue.Finish(ctx, k8sClient, heroA)).To(Succeed())
		Eventually(taintsOwnedBy(keyOf(heroB))).ShouldNot(BeEmpty())
	})

	It("aborts the drain when the hero can no longer fit even after eviction", func() {
		setupTopologyFixtures("256")
		// Make b2 permanently infeasible: a non-kueue pod owns all 16 GPUs.
		blocker := testingpod.MakePod("nonkueue-blocker", ns.Name).
			NodeName("b2-n1").RequestAndLimit(gpuRes, "8").
			StatusPhase(corev1.PodRunning).Obj()
		Expect(k8sClient.Create(ctx, blocker)).To(Succeed())
		blocker2 := testingpod.MakePod("nonkueue-blocker-2", ns.Name).
			NodeName("b2-n2").RequestAndLimit(gpuRes, "8").
			StatusPhase(corev1.PodRunning).Obj()
		Expect(k8sClient.Create(ctx, blocker2)).To(Succeed())

		victim := createVictim("abort-victim", 100, nodeB1N1, nodeB1N2)
		heroWL := createHero("hero-abort", 2, 1000)
		heroKey := keyOf(heroWL)

		By("draining b1 and suspending its victim")
		Eventually(taintsOwnedBy(heroKey)).Should(ConsistOf(nodeB1N1, nodeB1N2))
		Eventually(func() bool {
			v := getWL(keyOf(victim))()
			return v.Spec.Active != nil && !*v.Spec.Active
		}).Should(BeTrue())

		By("killing a b1 node so the hero can no longer fit anywhere")
		dead := &corev1.Node{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeB1N2}, dead)).To(Succeed())
		Expect(k8sClient.Delete(ctx, dead)).To(Succeed())

		By("aborting: victim reactivated unconditionally, taints removed")
		Eventually(eventsFor(heroWL)).Should(ContainElement(EventDrainAborted))
		Eventually(func() bool {
			v := getWL(keyOf(victim))()
			_, marked := v.Annotations[DeactivatedForAnnotation]
			return v.Spec.Active != nil && *v.Spec.Active && !marked
		}).Should(BeTrue())
		Eventually(taintsOwnedBy(heroKey)).Should(BeEmpty())
	})

	It("aborts the drain when the hero is demoted mid-drain", func() {
		setupTopologyFixtures("256")
		victim := createVictim("demote-victim", 100, nodeB1N1, nodeB1N2)
		// Rule b2 out deterministically: an equal-priority victim makes it
		// infeasible (feasibility needs every victim strictly below the
		// hero), so the drain must land on b1 where the test's victim is.
		createVictim("b2-blocker", 1000, "b2-n1", "b2-n2")
		heroWL := createHero("hero-demoted", 2, 1000)
		heroKey := keyOf(heroWL)

		Eventually(taintsOwnedBy(heroKey)).Should(ConsistOf(nodeB1N1, nodeB1N2))
		Eventually(func() bool {
			v := getWL(keyOf(victim))()
			return v.Spec.Active != nil && !*v.Spec.Active
		}).Should(BeTrue())

		By("removing the hero label from the ClusterQueue mid-drain")
		cq := &kueue.ClusterQueue{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: heroCQName}, cq)).To(Succeed())
		delete(cq.Labels, testCfg.HeroCQLabelKey)
		Expect(k8sClient.Update(ctx, cq)).To(Succeed())
		// CQ changes don't trigger workload reconciles; nudge via the hero.
		wl := getWL(heroKey)()
		patch := client.MergeFrom(wl.DeepCopy())
		if wl.Annotations == nil {
			wl.Annotations = map[string]string{}
		}
		wl.Annotations["test/nudge"] = "1"
		Expect(k8sClient.Patch(ctx, wl, patch)).To(Succeed())

		By("aborting: victim reactivated, taints removed")
		Eventually(eventsFor(heroWL)).Should(ContainElement(EventDrainAborted))
		Eventually(func() bool {
			v := getWL(keyOf(victim))()
			_, marked := v.Annotations[DeactivatedForAnnotation]
			return v.Spec.Active != nil && *v.Spec.Active && !marked
		}).Should(BeTrue())
		Eventually(taintsOwnedBy(heroKey)).Should(BeEmpty())
	})

	It("aborts the drain when quota shrinks below the hero mid-drain", func() {
		setupTopologyFixtures("256")
		victim := createVictim("quota-victim", 100, nodeB1N1, nodeB1N2)
		// Equal-priority blocker rules b2 out (see the demote spec).
		createVictim("b2-blocker", 1000, "b2-n1", "b2-n2")
		heroWL := createHero("hero-quota-shrink", 2, 1000)
		heroKey := keyOf(heroWL)

		Eventually(taintsOwnedBy(heroKey)).Should(ConsistOf(nodeB1N1, nodeB1N2))

		By("shrinking the hero CQ quota below the hero's request")
		cq := &kueue.ClusterQueue{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: heroCQName}, cq)).To(Succeed())
		cq.Spec.ResourceGroups[0].Flavors[0].Resources[0].NominalQuota = resource.MustParse("8")
		Expect(k8sClient.Update(ctx, cq)).To(Succeed())
		wl := getWL(heroKey)()
		patch := client.MergeFrom(wl.DeepCopy())
		if wl.Annotations == nil {
			wl.Annotations = map[string]string{}
		}
		wl.Annotations["test/nudge"] = "1"
		Expect(k8sClient.Patch(ctx, wl, patch)).To(Succeed())

		By("aborting: victim reactivated, taints removed")
		Eventually(eventsFor(heroWL)).Should(ContainElement(EventDrainAborted))
		Eventually(func() bool {
			v := getWL(keyOf(victim))()
			return v.Spec.Active != nil && *v.Spec.Active
		}).Should(BeTrue())
		Eventually(taintsOwnedBy(heroKey)).Should(BeEmpty())
	})

	It("reactivates a victim whose marker names a deleted hero", func() {
		setupTopologyFixtures("256")
		// Orphaned marker: suspended + marked for a hero that does not
		// exist, no taints anywhere (the race window's end state).
		victim := utiltesting.MakeWorkload("orphan-victim", ns.Name).
			Active(false).
			Annotation(DeactivatedForAnnotation, ns.Name+"/never-existed").
			PodSets(*utiltesting.MakePodSet("main", 1).Obj()).
			Obj()
		Expect(k8sClient.Create(ctx, victim)).To(Succeed())

		Eventually(func() bool {
			v := getWL(keyOf(victim))()
			_, marked := v.Annotations[DeactivatedForAnnotation]
			return v.Spec.Active != nil && *v.Spec.Active && !marked
		}).Should(BeTrue())
	})

	It("never cycles the hero its own drain serves", func() {
		setupTopologyFixtures("256")
		heroWL := createHero("hero-self", 2, 1000)
		heroKey := keyOf(heroWL)

		By("waiting for the drain to taint b1")
		Eventually(taintsOwnedBy(heroKey)).ShouldNot(BeEmpty())

		By("admitting the hero into the drained domain with pods on tainted nodes")
		Expect(fakekueue.Admit(ctx, k8sClient, heroWL, heroCQName,
			map[string]int32{nodeB1N1: 1, nodeB1N2: 1})).To(Succeed())
		for i, node := range []string{nodeB1N1, nodeB1N2} {
			pod := testingpod.MakePod(fmt.Sprintf("hero-self-%d", i), ns.Name).
				NodeName(node).
				RequestAndLimit(gpuRes, "8").
				Annotation(kueue.WorkloadAnnotation, "hero-self").
				StatusPhase(corev1.PodRunning).
				Obj()
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		}

		By("hero stays active and unmarked while its pods run on tainted nodes")
		Consistently(func() bool {
			wl := getWL(heroKey)()
			_, marked := wl.Annotations[DeactivatedForAnnotation]
			return (wl.Spec.Active == nil || *wl.Spec.Active) && !marked
		}, "3s").Should(BeTrue())
	})

	It("hands the drain slot to the queued hero promptly after teardown", func() {
		setupTopologyFixtures("256")
		heroA := createHero("hero-slot-a", 2, 2000)
		Eventually(taintsOwnedBy(keyOf(heroA))).ShouldNot(BeEmpty())

		heroB := createHero("hero-slot-b", 2, 1000)
		Eventually(eventsFor(heroB)).Should(ContainElement(EventDrainQueued))

		By("deleting hero-A: janitor tears down and nudges the drain controller")
		Expect(k8sClient.Delete(ctx, heroA)).To(Succeed())

		By("hero-B drains well before its 30s poll would fire")
		Eventually(taintsOwnedBy(keyOf(heroB)), "20s").ShouldNot(BeEmpty())
	})

	It("completes a partially-applied drain after restart (crash resume)", func() {
		setupTopologyFixtures("256")
		heroKey := types.NamespacedName{Namespace: ns.Name, Name: "hero-resume"}
		// Simulate a crash that tainted only one of the two b1 nodes.
		Expect(taint.EnsureTaint(ctx, k8sClient, nodeB1N1, testCfg.TaintKey, heroKey, heroCQName, testNow)).To(Succeed())

		createHero("hero-resume", 2, 1000)

		By("re-tainting the full selected set despite the pre-existing partial taint")
		Eventually(taintsOwnedBy(heroKey)).Should(ContainElements(nodeB1N1, nodeB1N2))
	})
})

// eventsFor lists event reasons recorded for the workload.
func eventsFor(wl *kueue.Workload) func() []string {
	return func() []string {
		events := &corev1.EventList{}
		Expect(k8sClient.List(ctx, events, client.InNamespace(wl.Namespace))).To(Succeed())
		var reasons []string
		for i := range events.Items {
			if events.Items[i].InvolvedObject.Name == wl.Name {
				reasons = append(reasons, events.Items[i].Reason)
			}
		}
		return reasons
	}
}
