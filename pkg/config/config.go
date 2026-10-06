// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

// Package config holds every operator-tunable knob for the hero drain
// controller. Kueue's configuration model: every knob lives in the optional
// --config YAML file, overlaying the built-in defaults; there are no
// per-knob command-line flags.
package config

import (
	"flag"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/yaml"
)

// DetectionMode selects how a Workload is recognized as stuck on a TAS
// no-fit (as opposed to quota or other admission problems). The controller
// must only drain when draining can help: a hero pending on quota gains
// nothing from evicting a domain, while a hero pending because no single
// topology domain has room does.
//
// Kueue records the not-admitted cause on the Workload's
// QuotaReserved=False condition, but the shape of that signal changed
// across versions:
//
//   - Kueue 0.16.x: the condition reason is the literal "Pending" for
//     EVERY not-admitted cause. Only the condition message distinguishes a
//     TAS no-fit, e.g.:
//     `couldn't assign flavors to pod set main: topology "block" doesn't
//     allow to fit any of 16 pod(s)`
//     (message built in kueue pkg/cache/scheduler/tas_flavor_snapshot.go,
//     wrapped by pkg/scheduler/flavorassigner; reason stamped in
//     pkg/scheduler/scheduler.go requeueAndUpdate). Message matching is
//     the only signal that exists on 0.16.
//   - Kueue 0.19+: granular condition reasons exist and are on by default
//     (feature gate UnadmittedWorkloadsObservability); a TAS no-fit is
//     reason "TopologyPlacementFailed". No text matching needed.
type DetectionMode string

const (
	// DetectionAuto matches either version's signal: the granular
	// TopologyPlacementFailed reason (kueue >= 0.19) OR the legacy
	// "Pending" reason combined with a TAS no-fit message (kueue 0.16).
	// Works on both, survives a kueue upgrade with no config change.
	DetectionAuto DetectionMode = "auto"
	// DetectionMessage matches only the legacy 0.16-style message text.
	// Pin this if a future kueue version ever makes the reason-based
	// match misfire.
	DetectionMessage DetectionMode = "message"
	// DetectionReason matches only the granular condition reason
	// (kueue >= 0.19). Strictest; the right choice after the cluster is
	// upgraded, when text matching should be dead code.
	DetectionReason DetectionMode = "reason"
)

// Weights blends the disruption cost terms used to rank candidate victims
// against each other. They must sum to 1.
//
// The weights never decide WHETHER a victim can be evicted — feasibility
// filtering already guarantees every victim in a candidate domain is below
// the hero's priority. They only decide WHICH feasible domain's victims are
// cheapest to disrupt. (The hero's own priority and pod count appear in the
// cost terms purely as normalizers to keep each term in [0, 1].)
type Weights struct {
	// Priority weighs how important a victim is: prefer draining domains
	// occupied by lower-priority workloads.
	Priority float64 `json:"priority"`
	// PodCount weighs how big a victim is: prefer disrupting fewer pods
	// (less interrupted work and requeue churn).
	PodCount float64 `json:"podCount"`
	// Runtime weighs how much progress a victim loses: prefer evicting
	// younger jobs over ones deep into a run.
	Runtime float64 `json:"runtime"`
}

// Config is the full controller configuration.
type Config struct {
	// TaintKey is the node taint key used to drain a domain for a hero.
	TaintKey string `json:"taintKey"`
	// DrainTimeout bounds how long a domain stays tainted while the hero
	// is still not admitted before the drain is abandoned.
	DrainTimeout metav1.Duration `json:"drainTimeout"`
	// NudgeInterval paces the scheduling-nudge annotation bumps on a
	// drained node while the drained domain is empty of victims and the
	// hero is still unadmitted (see taint.NudgeAnnotation for why kueue
	// needs the bump at all). Each bump makes kueue drop its no-fit
	// memory and re-evaluate every pending workload in the affected
	// cohorts — cheap once, churn if hammered — so the default repeats
	// slowly; the first bump of a drain always fires immediately.
	NudgeInterval metav1.Duration `json:"nudgeInterval"`
	// HeroCQLabelKey is the ClusterQueue label that marks hero queues;
	// the label value must be "true".
	HeroCQLabelKey string `json:"heroCQLabelKey"`
	// HeroPriorityClassName is the WorkloadPriorityClass heroes must use.
	HeroPriorityClassName string `json:"heroPriorityClassName"`
	// GPUResourceName is the extended resource the drain math is over.
	GPUResourceName corev1.ResourceName `json:"gpuResourceName"`
	// Weights blends the disruption cost terms.
	Weights Weights `json:"weights"`
	// RuntimeHalfLife is the saturation half-life for the runtime cost
	// term: a victim running exactly this long scores 0.5.
	RuntimeHalfLife metav1.Duration `json:"runtimeHalfLife"`
	// CrossCQMultiplier scales the cost of evicting a victim admitted
	// through a different ClusterQueue than the hero's (same-CQ is 1).
	CrossCQMultiplier float64 `json:"crossCQMultiplier"`
	// CrossCohortMultiplier scales the cost of evicting a victim admitted
	// through a ClusterQueue in a different Cohort than the hero's (same-CQ
	// is 1).
	CrossCohortMultiplier float64 `json:"crossCohortMultiplier"`
	// StuckDetection selects the TAS no-fit detection strategy; see
	// DetectionMode for the kueue version differences behind it. It is a
	// config knob (not hardcoded) because message matching is the
	// riskiest assumption in the design: if kueue rewording ever breaks a
	// mode, operators can pin another instead of waiting on a new build.
	StuckDetection DetectionMode `json:"stuckDetection"`
	// DryRun computes and reports drain decisions without tainting nodes
	// or touching victim workloads.
	DryRun bool `json:"dryRun"`
	// NonBlockingPodLabels marks non-Kueue-managed pods that do not block
	// domain capacity (e.g. transient hpc-verification pods that clear out
	// on their own). Normally a non-Kueue pod's GPU usage is counted as
	// non-reclaimable because suspending Kueue workloads cannot move it; a
	// pod carrying ANY one of these key=value labels is counted as
	// reclaimable capacity instead.
	NonBlockingPodLabels map[string]string `json:"nonBlockingPodLabels"`
}

// Default returns the configuration documented in docs/PLAN.md.
func Default() Config {
	return Config{
		TaintKey:              "hero.coreweave.com/taint",
		DrainTimeout:          metav1.Duration{Duration: 30 * time.Minute},
		NudgeInterval:         metav1.Duration{Duration: 2 * time.Minute},
		HeroCQLabelKey:        "hero.coreweave.com/enabled",
		HeroPriorityClassName: "hero-critical",
		GPUResourceName:       "nvidia.com/gpu",
		Weights:               Weights{Priority: 0.7, PodCount: 0.2, Runtime: 0.1},
		RuntimeHalfLife:       metav1.Duration{Duration: 6 * time.Hour},
		CrossCQMultiplier:     5,
		CrossCohortMultiplier: 10,
		StuckDetection:        DetectionAuto,
		DryRun:                false,
		NonBlockingPodLabels:  map[string]string{},
	}
}

// Load resolves the configuration: defaults, overlaid by the optional YAML
// file named by --config. Kueue's model: ALL operational knobs live in the
// file; the command line carries only bootstrap flags (--config here, plus
// whatever the caller registers on fs — manager, logging). No per-knob
// flags, so no flag-vs-file precedence exists. args is the command line
// after the program name (typically os.Args[1:]).
func Load(fs *flag.FlagSet, args []string) (Config, error) {
	cfg := Default()

	var configPath string
	fs.StringVar(&configPath, "config", "", "Path to a YAML configuration file; omit to run on defaults.")

	if err := fs.Parse(args); err != nil {
		return cfg, err
	}

	if configPath != "" {
		if err := cfg.loadYAML(configPath); err != nil {
			return cfg, err
		}
	}

	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *Config) loadYAML(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading config file: %w", err)
	}
	if err := yaml.UnmarshalStrict(data, c); err != nil {
		return fmt.Errorf("parsing config file %s: %w", path, err)
	}
	return nil
}

// Validate rejects configurations the controller must not run with.
func (c *Config) Validate() error {
	var errs []string

	for _, msg := range validation.IsQualifiedName(c.TaintKey) {
		errs = append(errs, fmt.Sprintf("taintKey %q: %s", c.TaintKey, msg))
	}
	for _, msg := range validation.IsQualifiedName(c.HeroCQLabelKey) {
		errs = append(errs, fmt.Sprintf("heroCQLabelKey %q: %s", c.HeroCQLabelKey, msg))
	}
	if c.HeroPriorityClassName == "" {
		errs = append(errs, "heroPriorityClassName must not be empty")
	}
	for _, msg := range validation.IsQualifiedName(string(c.GPUResourceName)) {
		errs = append(errs, fmt.Sprintf("gpuResourceName %q: %s", c.GPUResourceName, msg))
	}
	if c.DrainTimeout.Duration <= 0 {
		errs = append(errs, "drainTimeout must be positive")
	}
	if c.NudgeInterval.Duration <= 0 {
		errs = append(errs, "nudgeInterval must be positive")
	}
	if c.RuntimeHalfLife.Duration <= 0 {
		errs = append(errs, "runtimeHalfLife must be positive")
	}
	if c.Weights.Priority < 0 || c.Weights.PodCount < 0 || c.Weights.Runtime < 0 {
		errs = append(errs, "weights must be non-negative")
	}
	if sum := c.Weights.Priority + c.Weights.PodCount + c.Weights.Runtime; math.Abs(sum-1) > 1e-9 {
		errs = append(errs, fmt.Sprintf("weights must sum to 1, got %v", sum))
	}
	if c.CrossCQMultiplier < 1 {
		errs = append(errs, "crossCQMultiplier must be >= 1 (same-CQ cost is 1)")
	}
	if c.CrossCohortMultiplier < 1 {
		errs = append(errs, "crossCohortMultiplier must be >= 1 (same-CQ cost is 1)")
	}
	switch c.StuckDetection {
	case DetectionAuto, DetectionMessage, DetectionReason:
	default:
		errs = append(errs, fmt.Sprintf("stuckDetection must be one of auto, message, reason; got %q", c.StuckDetection))
	}
	for k, v := range c.NonBlockingPodLabels {
		for _, msg := range validation.IsQualifiedName(k) {
			errs = append(errs, fmt.Sprintf("nonBlockingPodLabels key %q: %s", k, msg))
		}
		for _, msg := range validation.IsValidLabelValue(v) {
			errs = append(errs, fmt.Sprintf("nonBlockingPodLabels[%s] value %q: %s", k, v, msg))
		}
	}

	if len(errs) > 0 {
		slices.Sort(errs)
		return fmt.Errorf("invalid configuration: %s", strings.Join(errs, "; "))
	}
	return nil
}
