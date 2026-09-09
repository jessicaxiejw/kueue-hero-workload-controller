# Implementation Plan

Status legend: `[ ]` pending · `[~]` in progress · `[x]` done

## 1. What and why

The customer runs Kueue 0.16.9 with Topology Aware Scheduling. "Hero" workloads sit Pending because within-quota workloads from other ClusterQueues are scattered across every topology domain — no single domain fits the hero, and Kueue can't preempt them by design.

This controller is the stopgap from the design doc *Hero Workloads on Kueue TAS: Taint-Based Domain Drain*: when a hero is stuck on TAS no-fit, pick the cheapest-to-drain topology domain, taint its nodes, suspend/unsuspend the victims so Kueue requeues them elsewhere, let the taint-tolerating hero in, then untaint. Taints are self-cleaning so a crash can never permanently brick a domain.

## 2. Shape of the solution

- **One process, one Deployment — two reconcile loops** on the same controller-runtime manager (shared cache/client/leader-election):
  - **Drain controller** — watches Workloads; detects stuck heroes, picks a domain, taints it, cycles victims (design doc phases 0–2).
  - **Janitor controller** — watches Nodes carrying our taint; removes taints when the hero is placed / finished / abandoned / timed out, and garbage-collects orphaned taints (phase 4 + leak protection).
- **No CRDs, no database.** All state lives in the cluster: the drain taint marks the drain in flight, with the hero's **ClusterQueue as the taint value** so only that CQ's heroes tolerate the drained domain (Equal-on-own-CQ tolerations keep other CQs' heroes out of partially-freed capacity; per-workload values are impossible since kueue generates workload names after tolerations are written). Node annotations record the owning hero workload and the drain start time. Restart-safe by construction.
- **Victims evicted the supported way:** toggle Kueue Workload `spec.active` false → true. Kueue evicts, suspends the Job/JobSet, requeues with no backoff penalty.
- **All decision math is pure Go** (no cluster access) so the risky parts — feasibility, cost heuristic, domain selection — are exhaustively unit-testable.

## 3. Milestones

| # | Status | Milestone | Proves itself by |
|---|---|---|---|
| M0 | `[x]` | Scaffold: kubebuilder init, kueue 0.16.9 dep, Makefile, CI | envtest boots with kueue CRDs |
| M1 | `[x]` | Config package (taint key, weights, timeout, dry-run…) | unit tests |
| M2 | `[x]` | Hero identification + "stuck on TAS" detection (pure) | table-driven unit tests |
| M3 | `[x]` | Domain snapshot: nodes/pods → per-domain capacity & victims (pure) | table-driven unit tests |
| M4 | `[x]` | Feasibility + disruption-cost heuristic + domain selection (pure) | golden cases from design doc |
| M5 | `[x]` | Taint apply/remove with ownership encoding | fake-client unit + envtest |
| M6 | `[x]` | One-drain-at-a-time discovery + hero ordering (pure) | unit tests |
| M7 | `[x]` | Drain controller wired end-to-end (phases 0–2) | envtest with simulated kueue |
| M8 | `[x]` | Janitor controller (phase 4 untaint paths + leak GC) | envtest, one test per untaint trigger |
| M9 | `[x]` | Metrics + structured logging | metric assertions in existing tests |
| M10 | `[x]` | Helm chart, RBAC, release CI | helm lint / template in CI |
| M11 | `[x]` | kind e2e against real kueue (version matrix) | full drain lifecycle; run against 0.16.9, 0.18.6, 0.19.2 |

## 4. Decisions already settled

- **Compatible with kueue 0.16.9 (customer's current version) through the 0.19 releases.** Pin the `sigs.k8s.io/kueue v0.16.9` Go module and build against **v1beta2**: verified by diffing the tags, the v1beta2 wire format is purely additive from 0.16.9 through the current development branch (0.20-devel) for every field we use (`spec.priorityClassRef`, `spec.active`, compressed `TopologyAssignment`, conditions, CQ quotas, Topology levels), so one binary serves all of them. E2e-verified against 0.16.9, 0.18.6, and 0.19.2 (0.20 is unreleased as of 2026-08-28).
- **Version-spanning stuck detection.** 0.16.9 stamps every pending Workload with condition reason literal `"Pending"`; 0.19+ uses granular `TopologyPlacementFailed`. Detection mode `auto` (default) treats a Workload as TAS-stuck if reason == `TopologyPlacementFailed` **or** (reason == `Pending` and message matches `couldn't assign flavors to pod set …: topology … doesn't allow to fit`). Explicit `message`/`reason` modes remain for pinning.
- **`kubebuilder init` scaffold**; unused CRD/webhook machinery deleted.
- **OPA policies out of scope** (follow-up, lives with cluster policy).
- **kind e2e included** as final milestone — validates message-match detection against real kueue output, the riskiest assumption.
- **Scheduling nudge, node-side (found by e2e).** Kueue 0.16.9 retries a parked (inadmissible) workload only on kueue events — victim pods finishing termination is not one — AND it memorizes each rejected workload shape (`noFitSchedulingHashes`), silently re-parking updates of a memorized shape without a scheduling attempt, so even hero Workload updates cannot wake it (verified in an e2e run: 12 workload-annotation bumps over 3 minutes, zero admissions). The one eraser for that memory is a node change: kueue's TAS node watcher treats ANY change to a TAS-flavor node — annotations included, unfiltered — as "capacity may have changed", wipes the NoFit memory, and re-heaps every parked workload (`tas/resource_flavor.go` → `NotifyRetryInadmissible`); that's why heroes admit right after the timeout teardown's untaint. The drain controller therefore bumps a `hero.coreweave.com/scheduling-nudge` annotation (RFC3339 timestamp, doubling as the rate limiter) on one drained node once the drained nodes are empty of victims and the hero still lacks quota reservation — first bump immediate, repeats paced by config `nudgeInterval` (default 2m; deliberately slow since every bump re-evaluates all pending workloads — the repeats only insure against kueue's node-event batching racing the first bump). Stripped by `RemoveOwnedTaint` with the other drain annotations. Full rationale on `taint.NudgeAnnotation`.

  **Known limitation (kueue 0.16.9 through v0.20-devel, verified in e2e + source):** kueue delivers node events to its TAS scheduler through a 10-slot channel with BLOCKING sends (`pkg/controller/tas/resource_flavor.go`, `nodeUpdateCh`). A drain-start burst (36+ taint writes in ~1s) overfills it and kueue goes deaf to ALL node changes — the nudge included — until the backlog flushes (observed ~3min; flush trigger unidentified). The nudge is never lost, only delayed: it rides the flush, and admission follows within a second. Net behavior: small drains (under ~10 node writes) get nudged in seconds; large drains wait for the flush, bounded as ever by `drainTimeout`. Do NOT remove the nudge over this — without it even healthy small drains stall the full `drainTimeout`, since victim-pod termination generates no kueue event at all.

## 5. Milestone detail

### M0 — Scaffold, build, CI
`kubebuilder init` (module `github.com/coreweave/kueue-hero-workload-controller`; Go version matching kueue 0.16.9's toolchain). Strip CRD/webhook scaffolding. Add `sigs.k8s.io/kueue v0.16.9`, register v1beta2 scheme. Makefile: `build`, `test` (setup-envtest), `lint`, `docker-build`, `kueue-crds` (pin 0.16.9 CRD YAMLs into `test/crds/`). CI workflow with SHA-pinned actions (org convention). Multi-stage distroless Dockerfile.
**Done when:** `make build lint test` green; suite_test boots envtest with kueue CRDs and a bare manager.

### M1 — Config
`pkg/config/`: `TaintKey` (default `hero.coreweave.com/taint`), `DrainTimeout` (30m), `HeroCQLabelKey` (`hero.coreweave.com/enabled`), `HeroPriorityClassName` (`hero-critical`), `GPUResourceName` (`nvidia.com/gpu`), weights `{0.7, 0.2, 0.1}`, `RuntimeHalfLife` (6h), `CrossCQMultiplier` (5), `StuckDetectionMode` (`message`|`reason`), `DryRun`, `NonBlockingPodLabels` (hpc-verification carve-out; ANY-match). Flags + optional `--config` YAML from ConfigMap; no hot reload. **Tests:** defaults, precedence, validation.

### M2 — Hero identification & stuck detection (pure)
`pkg/hero/`:
- `IsHero(wl, cq, cfg)` — CQ label `hero.coreweave.com/enabled=true`; `spec.priorityClassRef` is a `WorkloadPriorityClass` named `hero-critical`; every podset carrying the slice pair (`podset-slice-required-topology` + `podset-slice-size`) tolerates the taint key — podsets without the slice pair are never placed in a drained domain, so their tolerations are not checked.
- `IsStuckTASNoFit(wl, mode)` — strategy interface: `auto` (default; reason `TopologyPlacementFailed` OR `Pending`+message-match) / `message` / `reason`.
- `RequiredTopologyLevels(wl)`: the drain trigger is the SLICE PAIR only — `topologyRequest.podSetSliceRequiredTopology` + `podSetSliceSize` both set (customer-confirmed hero contract; plain `required` never triggers). `CoarsestLevel` picks the drain level when podsets disagree (Topology `spec.levels` is ordered highest→lowest). `GPURequest(wl)`; `HeroPriority(wl)` from `spec.priority`.
- Test fixtures: kueue's own wrappers (`sigs.k8s.io/kueue/pkg/util/testing/v1beta2`).

**Tests:** each identifier present/absent; real 0.16.9 message variants (`doesn't allow to fit any of`, `allows to fit only`, multi-podset, `slice(s)` unit, truncated); quota-pending must NOT match.

### M3 — Domain snapshot (pure)
`pkg/snapshot/`: `Build(nodes, pods, workloads, level, cfg)` → domains keyed by topology label value at the hero's required level, each with: allocatable GPU (healthy, non-foreign-tainted nodes), non-reclaimable GPU usage, victim list, `HostsHero` flag.
- Non-reclaimable: DaemonSet/static pods, pods tolerating existing taints, non-kueue-managed pods (no `kueue.x-k8s.io/workload` annotation) unless carrying any `NonBlockingPodLabels` entry.
- Victims: only GPU-requesting pods on GPU nodes (CPU-only co-tenants don't block).
- `HostsHero`: decode other heroes' `TopologyAssignment` (reuse kueue `pkg/util/tas` `InternalFrom`/`DomainID`). Decode EVERY `podSetAssignments[i]` — TAS assigns each podset independently, so one workload's podsets can occupy different domains (verified in kueue's TAS integration tests at v0.16.9).

**Tests:** pod-classification mixes, unhealthy/pre-tainted nodes, non-blocking-label carve-outs, multi-level topology, fractional GPU math.

### M4 — Feasibility, cost, selection (pure)
`pkg/selection/`. The selector picks a min-cost **domain set**, generalizing
the design doc's single-domain drain to cover slice-only heroes:

- **Demand chunks.** Whole-podset `required` hero: demand = 1 chunk =
  the full GPU sum, so the set is always a single domain (the design doc
  case). Slice-only hero (`podSetSliceRequiredTopology` without
  `required`): chunk = slice GPU request (sliceSize × per-pod GPU),
  demand = slice count; slices may spread, so the drain may taint several
  slice-level domains. A domain contributes
  `floor(reclaimable / chunk)` chunks — fragments smaller than a
  chunk contribute nothing.
- `Feasible(domain, hero)`: contributes ≥ 1 chunk; every victim
  priority < hero priority; no other hero assigned into the domain.
- `Cost(victimWorkload)` = `m_rel · (w_p·p̂ + w_n·n̂ + w_r·r̂)`; p̂ =
  prio/heroPrio, n̂ = pods/(pods+heroPods), r̂ = runtime/(runtime+halfLife);
  m_rel = 5 cross-CQ, 1 same-CQ. Victim `podCount_i` and usage come from
  admitted state (`podSetAssignments[].count`, actual pods on nodes) — not
  spec `count`; partial admission (`minCount`) makes them differ. The
  hero side always uses spec counts (full ask; a pending hero has no
  admitted state, and draining for `minCount` would invite a downsized
  hero).
- `SelectDomains` → `Selected | NoFeasibleDomains | AllFeasibleHeroOccupied`;
  greedy cheapest-cost-per-chunk until demand covered; lexicographic
  tie-break for determinism. All selected domains are tainted under the
  same owner value — one hero, one drain, N domains; janitor semantics
  unchanged.

**Tests:** golden cases from the design doc, determinism, empty-domain-wins,
monotonicity properties, hero-occupied distinct outcome; slice-only cases:
1-slice-per-rack → N racks, multi-slice rack → fewer racks, fragments
excluded, insufficient total chunks → `NoFeasibleDomains`.

### M5 — Taint operations
`pkg/taint/`: `OwnerValue`/`ParseOwnerValue` (`<ns>_<name>`, taint-value charset/length validation); `EnsureTaint` (read-modify-write + `retry.RetryOnConflict` — NOT SSA, since `node.spec.taints` is atomic under SSA and we'd fight kubelet; writes `drain-started-at` annotation in the same update); `RemoveOwnedTaint` (only if key AND owner match); `FindHeroTaints`.
**Tests:** idempotency, foreign taints untouched, malformed values; envtest conflict retry.

### M6 — Drain serialization (pure)
`pkg/drain/`: `Discover(nodes, key)` rebuilds in-flight drains from taints + annotations; `NextHero(candidates)` orders by priority desc → age asc → name. Reconcile proceeds only if this hero is next AND no foreign drain in flight — one drain per cluster, no queue data structure needed. Missing `drain-started-at` → rewrite as now.
**Tests:** ordering tables; discovery with stale/foreign/multiple taints.

### M7 — Drain controller (phases 0–2), envtest
`pkg/controller/drain/`. Reconcile (idempotent, stateless): predicates → resume own drain if taint names me → quota precondition (hero request ≤ hero CQ nominalQuota; violation = event + stop) → serialization gate → snapshot (resolve Topology via CQ → ResourceFlavor → `spec.topologyName`) → selection (each outcome → distinct event; DryRun stops before tainting) → taint all domain nodes → victim cycling: assigned-to-domain + active → `active=false` + `deactivated-for` annotation; annotated + `Evicted=Deactivated` → `active=true`. Watches: Workload (primary), Node (mapped to stuck heroes), nudge channel from janitor. Indexers: pods-by-node, pods-by-workload-annotation, workloads-by-CQ.
`test/utils/fakekueue/` simulates kueue in envtest: stamps pending conditions with real 0.16.9 message text, reacts to `active=false` by evicting.
**Tests:** happy path to tainted domain; victim toggle round-trip; over-quota → event only; second hero queues; dry-run; manager-restart mid-drain completes (crash safety); events on hero.

### M8 — Janitor controller (phase 4), envtest
`pkg/controller/janitor/`. Primary Node (predicate: has our taint key). Untaint at the first of: **abandoned** (hero absent / deactivated / evicted-without-requeue) · **finished** · **placed elsewhere** (decoded assignment domain ≠ node's domain — untaint at admission) · **placed** (admitted into drained domain AND Running hero pods ≥ Σ podset counts — not mere admission) · **timeout** (`now − drain-started-at > DrainTimeout` → untaint + `DrainTimedOut` event; `RequeueAfter` = remaining deadline. The deadline bounds the WHOLE journey — taint → eviction → admission → all pods Running — so an admitted hero whose pods never all start cannot pin the drained domain forever. Deliberately NO retry backoff — deviation from the design doc, chosen for scheduling simplicity: the hero is immediately eligible again, and repeated timeouts surface via events and the drains-timed-out metric instead of a cooling-off mechanism). Orphan GC doubles as structural leak protection. Last untaint → nudge drain controller.

**Victim sweep at teardown:** whenever the janitor ends a drain (any trigger above), it also reactivates every workload annotated `deactivated-for: <that hero>` — **unconditionally**, not waiting for the Evicted condition. Closes the stranding gap where kueue never completes an eviction (kueue down mid-drain, victim finished as it was suspended): setting `active=true` on a never-evicted victim is a harmless no-op, on an evicted one it is the normal requeue. Reactivation always removes the `deactivated-for` annotation in the same patch (reuse the drain controller's `reactivateVictim`) so the marker never leaks into future logic. Invariant: no taint of a drain survives its end, and no victim of a drain stays suspended or marked past its end.

**Tests:** one per trigger; foreign taint untouched; hero deleted mid-drain → full GC; missing-annotation fallback; teardown sweep reactivates a victim whose eviction never completed.

### M9 — Observability
`pkg/metrics/` on controller-runtime's global registry: `hero_drains_started_total`, `hero_drains_completed_total{outcome}`, `hero_drains_timed_out_total` (the no-backoff alert signal promised in docs), `hero_drains_in_flight`, `hero_selection_outcomes_total{outcome}`, `hero_victims_suspended_total`, `hero_victims_reactivated_total`. Drain counters count drains, not nodes: completion increments only when the drain's LAST taint is removed (outcome = last node's teardown reason). Structured logs at drain start/abort with domains/nodes/victims/cost. **Tests:** `testutil.ToFloat64` delta assertions inside the existing envtest specs (happy path: started/planned/suspended/reactivated; timeout: timed-out + completed{timed_out}).

### M10 — Packaging
Helm chart at `charts/kueue-hero-workload-controller/`: Deployment (config mounted from ConfigMap via `--config`, config-checksum rollout annotation, probes, restricted securityContext), ConfigMap (full controller config from `values.yaml` `config:` block), RBAC (ClusterRole mirroring the kubebuilder markers — generated copy at `config/rbac/role.yaml`; keep in sync — plus leader-election Role/RoleBinding), ServiceAccount, optional metrics Service. Release CI / Artifactory publishing deliberately dropped (user decision 2026-08-19); chart is installed from the repo. **Tests:** `make helm-lint` (helm lint + template render, defaults and minimal variant) locally and in the lint workflow.

### M11 — kind e2e, real kueue (version matrix)
kind cluster; nodes labeled with fake block/rack topology; fake `nvidia.com/gpu` allocatable via status subresource; kueue chart with TAS, version parameterized — run against **0.16.9 and the latest release** (0.18.6 and 0.19.2 verified). Scenarios: full happy path (scattered fillers → hero pending with real no-fit signal → drain → fillers requeue → hero Running → untaint); timeout path; controller restart mid-drain. Validates auto stuck-detection against both versions' real output — the riskiest assumption. `make test-e2e KUEUE_VERSION=…`, nightly/manual CI.

## 6. Risk → test map

| Risk | Covered |
|---|---|
| Message-match breaks (0.16.9 wording) | M2 tables use real strings; M11 real kueue |
| Controller crash mid-drain | M7 restart test; M11 |
| Hero deleted mid-drain | M8 abandoned GC |
| Second hero during drain | M6 ordering; M7 queue test |
| Taint stolen / foreign value | M5; M8 |
| Victims never vacate | M8 timeout; M11 |
| Nodes added/removed mid-drain | M7 idempotent taint; janitor GC |
| Kueue upgrade to 0.18/0.19 | M2 auto-detection covers both signal styles; M11 version matrix |

## 7. Future work: incremental eviction (over-drain reduction)

Not in scope for the initial milestones; documented here so the follow-up
has its design context.

**The problem.** The drain evicts every victim in the selected domain
(design doc Phase 2), even when the hero needs far less than the domain's
full capacity: hero needs 128 GPUs, block has 96 free and 300 held by
victims → deficit is 32, but all 300 are evicted. Whole-domain eviction is
the dominant over-drain source; two minor ones (sum-sizing multi-podset
heroes into one domain, coarsest-level drains for disagreeing podset
levels) are bounded by the hero's own size and rare inputs respectively.

**Why v1 ships whole-domain anyway.**
- Deterministic and idempotent: "victims" = "everything in the domain";
  the taint alone reconstructs drain state after a crash.
- Guaranteed convergence: feasibility already proved the emptied domain
  fits the hero. Partial eviction must reason about capacity *shape* —
  8-GPU hero pods need whole-node holes, and freeing 32 GPUs as 4-GPU
  fragments admits nothing. Getting that wrong stalls the drain into
  timeout: victims evicted AND hero never fully running (covers both
  never-admitted and admitted-but-pods-never-start).
- The cost heuristic already bounds the pain: blast radius is fixed at one
  domain, so selection optimizes which domain suffers.

**Sketch (follow-up).** Config `evictionMode: domain | incremental`
(default `domain`):
1. deficit = hero GPU request − domain's current free capacity.
2. Taint the whole domain either way — blocking backfill is cheap and
   reversible; eviction is the disruptive part.
3. Rank the domain's nodes by the summed disruption cost of the victim
   pods on them; suspend victims on the cheapest nodes until freed
   whole-node capacity covers the deficit.
4. Watch-driven loop: hero admitted → untaint, surviving victims keep
   running. Hero still stuck after the tranche settles → evict the
   next-cheapest tranche. The existing drain timeout still bounds the
   whole attempt.

Crash-safety carries over unchanged: the `deactivated-for=<hero>`
annotation already marks exactly which victims a drain owns.

**Prerequisite before building it:** evidence that over-eviction hurts in
practice — watch `hero_victims_suspended_total` against actual
hero deficits in production drains.

## 8. Key references

- Design doc: *Hero Workloads on Kueue TAS: Taint-Based Domain Drain (Stopgap)*
- Kueue at tag `v0.16.9`: `apis/kueue/v1beta2/workload_types.go` (conditions, `spec.active`, `priorityClassRef`) · `pkg/util/tas/tas_assignment.go` (assignment decode) · `pkg/scheduler/scheduler.go` `requeueAndUpdate` (the `"Pending"` reason) · `pkg/scheduler/flavorassigner/flavorassigner.go` + `pkg/cache/scheduler/tas_flavor_snapshot.go` (no-fit message wording)
