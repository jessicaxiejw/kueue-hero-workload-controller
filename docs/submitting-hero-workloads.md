# Submitting a Hero Workload

A "hero" workload must start promptly even when other tenants' workloads
are scattered across every topology domain. The controller watches for
heroes stuck on a topology no-fit and clears the cheapest domain for them
(see [ARCHITECTURE.md](ARCHITECTURE.md) for how).

A workload is a hero only if it matches all three identifiers below. Miss
any one and it queues like everything else; no drain happens.

## Prerequisites (cluster admin, once)

1. **A hero-enabled ClusterQueue.** Tenants cannot self-declare heroes:
   the gate is a label on the admin-controlled ClusterQueue, and its
   nominal quota must cover the largest hero (heroes never borrow).

   ```yaml
   apiVersion: kueue.x-k8s.io/v1beta2
   kind: ClusterQueue
   metadata:
     name: hero-cq
     labels:
       hero.coreweave.com/enabled: "true"   # controller's HeroCQLabelKey
   ```

2. **The hero WorkloadPriorityClass.** The value must exceed every
   victim's priority: a domain is only drainable if every victim in it
   ranks strictly below the hero.

   ```yaml
   apiVersion: kueue.x-k8s.io/v1beta2
   kind: WorkloadPriorityClass
   metadata:
     name: hero-critical                    # controller's HeroPriorityClassName
   value: 1000000
   ```

3. **A LocalQueue** in the tenant namespace pointing at the hero
   ClusterQueue.

## Submitting (workload author)

Sample hero workload. The three required pieces are explained below.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: hero-training-run
  namespace: team-a
  labels:
    kueue.x-k8s.io/queue-name: team-a-hero-queue     # LocalQueue -> hero CQ
    kueue.x-k8s.io/priority-class: hero-critical     # WorkloadPriorityClass
spec:
  suspend: true            # kueue-managed jobs start suspended
  completions: 16
  parallelism: 16
  completionMode: Indexed
  template:
    metadata:
      annotations:
        # Each 8-pod slice must fit one domain at this topology level
        # (node label key from the cluster's kueue Topology object):
        kueue.x-k8s.io/podset-slice-required-topology: cloud.provider.com/topology-rack
        kueue.x-k8s.io/podset-slice-size: "8"
        # Optional but recommended: ALSO pin the whole workload into one
        # domain of a higher level, so the slices stay together:
        kueue.x-k8s.io/podset-required-topology: cloud.provider.com/topology-block
    spec:
      restartPolicy: Never
      tolerations:
        - key: hero.coreweave.com/taint                 # controller's TaintKey
          operator: Equal
          value: hero-cq                                # your ClusterQueue's name
      containers:
        - name: trainer
          image: registry.example.com/trainer:latest
          resources:
            requests:
              nvidia.com/gpu: 8
            limits:
              nvidia.com/gpu: 8
```

Three things make it a hero:

1. **Route it to the hero queue.** Two labels: `queue-name` (a LocalQueue
   backed by the hero ClusterQueue) and `priority-class` (`hero-critical`).
2. **Say what shape of space it needs.** Two annotations:
   `podset-slice-required-topology` (each slice fits inside one domain at
   this level) and `podset-slice-size` (pods per slice). This pair is what
   the controller drains for; without it, no drain ever happens.
3. **Tolerate the drain taint.** The controller frees space by tainting
   nodes; your pods must tolerate that taint or they cannot use the space.
   Use `operator: Equal` with your own ClusterQueue name as the value, so
   your hero only enters domains drained for your queue. Only the podsets
   carrying the slice pair are required to tolerate it — a podset without
   the slice pair is never placed in a drained domain, so its tolerations
   are not checked.

Topology notes:

- Slices are independent by default: each fits one rack, but two slices
  may land in different blocks, and the controller drains the same way —
  it packs the cheapest racks it can find anywhere at that level. To keep
  the whole workload inside one higher-level domain, add
  `podset-required-topology: <higher level>` on top of the slice pair, as
  in the example; the controller then confines the drain to a single
  domain at that level too. The drain still triggers from the slice pair.
  Be aware this is a real constraint on feasibility: a hero needing more
  racks than any one block holds is undrainable with it and drainable
  without it.
- `podset-required-topology` ALONE never triggers a drain.
- Do not use `podset-slice-required-topology-constraints`: kueue 0.16.9
  silently ignores it.

For a JobSet, the same labels go on the JobSet metadata and the annotation +
toleration go on **each** replicated job's pod template that carries the
slice pair. Replicated jobs without the slice pair (an untethered leader,
a sidecar) do not need the toleration to pass hero identification — but add
it anyway if you expect them to land in the drained domain.

**JobSet co-placement (strongly recommended).** Each replicated job places
independently by default: after a drain, TAS may put an untethered leader
in a different domain than the workers. Add the same
`kueue.x-k8s.io/podset-group-name` annotation to every replicated job's
pod template so kueue places them together in the drained domain:

```yaml
    annotations:
      kueue.x-k8s.io/podset-required-topology: cloud.provider.com/topology-block
      kueue.x-k8s.io/podset-group-name: my-training-group   # same value everywhere
```

Notes: each grouped podset must also carry
`kueue.x-k8s.io/podset-required-topology`; groups are designed for two
podsets (leader + workers); grouping cannot be combined with the slice
annotations (kueue's webhook rejects that).

## What happens after submission

1. Kueue creates a Workload; if quota is free but no topology domain fits,
   the Workload stays pending with a topology no-fit condition.
2. The controller detects the stuck hero, picks the feasible domain whose
   victims are cheapest to disrupt, taints that domain's nodes, and
   suspends/unsuspends the victims so Kueue requeues them elsewhere.
3. TAS admits the hero into the cleared domain (the toleration lets it in).
4. Once all hero pods are Running (or the hero finishes, is deleted, or the
   drain times out), the taint is removed.

One drain runs at a time per cluster; additional stuck heroes queue by
priority, then age.

## Troubleshooting

The controller emits Events on the hero Workload (`kubectl describe
workload -n <ns>`). If nothing happens at all, check the identifiers in
order:

| Symptom | Check |
|---|---|
| No events, no drain | ClusterQueue label `hero.coreweave.com/enabled: "true"` present? |
| No events, no drain | Workload's `spec.priorityClassRef` names `hero-critical` with kind `WorkloadPriorityClass`? (`kueue.x-k8s.io/priority-class` label on the Job, not the pod PriorityClass) |
| No events, no drain | Every slice-pair podset's pod template tolerates the taint key? |
| Event `HeroExceedsQuota` | Hero request exceeds the CQ's nominal quota, a customer-side commitment; shrink the job or raise quota |
| Event `NoFeasibleDomains` | No domain can fit the hero even after eviction (capacity, priorities, or another hero occupies every candidate) |
| Event `DrainQueued` | Another hero's drain is in flight; this one is queued |
| Event `DrainTimedOut` | The drain didn't converge within the timeout: the hero was not fully Running by the deadline (never admitted, or admitted but its pods never all started); taints removed, the drain re-evaluates immediately. Repeated `DrainTimedOut` events on one hero mean something structural; investigate rather than wait |
| Event `DrainAborted` | Mid-drain the hero stopped fitting even after full eviction (e.g. a node died); victims were reactivated, taints removed, and the drain will be re-evaluated |

Note the controller only reacts to **topology** no-fit. A hero pending on
quota (`insufficient quota` in the condition message) is intentionally left
alone; draining cannot create quota.

## Metrics

Prometheus metrics on the manager's metrics endpoint. Drain counters count
drains, not nodes: a drain spans many tainted nodes but starts once and
completes once.

| Metric | Type | Meaning |
|---|---|---|
| `hero_drains_started_total` | counter | Drains started (domains tainted for a stuck hero) |
| `hero_drains_completed_total{outcome}` | counter | Drains fully torn down, by outcome: `placed`, `placed_elsewhere`, `finished`, `abandoned`, `deactivated`, `timed_out`, `aborted` |
| `hero_drains_timed_out_total` | counter | Drains that never converged within the drain timeout. **Alert on this** |
| `hero_drains_in_flight` | gauge | Drains currently holding taints (0 or 1 in steady state; drains are serialized) |
| `hero_selection_outcomes_total{outcome}` | counter | Domain-selection attempts: `planned`, `no_feasible_domains`, `all_feasible_hero_occupied` |
| `hero_victims_suspended_total` | counter | Victim workloads suspended to make room for a hero |
| `hero_victims_reactivated_total` | counter | Victim workloads handed back after a drain |

Alerting rules, what makes drains fail, and how to stop a failing hero:
see [alerting.md](alerting.md). The short version: there is no automatic
backoff after a timed-out drain, so a hero whose drains can never succeed
retries every `drainTimeout` until an operator steps in, and the metric
above is the signal.
