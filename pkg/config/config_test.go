// SPDX-FileCopyrightText: 2026 CoreWeave, Inc.
// SPDX-License-Identifier: Apache-2.0
// SPDX-PackageName: kueue-hero-workload-controller

package config

import (
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func loadArgs(t *testing.T, args ...string) (Config, error) {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return Load(fs, args)
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaultsAreValid(t *testing.T) {
	cfg, err := loadArgs(t)
	if err != nil {
		t.Fatalf("Load with no args: %v", err)
	}
	want := Default()
	if cfg.TaintKey != want.TaintKey ||
		cfg.DrainTimeout != want.DrainTimeout ||
		cfg.StuckDetection != DetectionAuto ||
		cfg.CrossCQMultiplier != 5 {
		t.Errorf("Load without inputs diverged from Default(): got %+v", cfg)
	}
}

func TestPrecedence(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want func(t *testing.T, c Config)
	}{
		{
			name: "file overrides default",
			yaml: "taintKey: example.com/from-file\ndryRun: true\n",
			want: func(t *testing.T, c Config) {
				if c.TaintKey != "example.com/from-file" {
					t.Errorf("TaintKey = %q", c.TaintKey)
				}
				if !c.DryRun {
					t.Error("DryRun not applied from file")
				}
				// Untouched knobs keep defaults.
				if c.HeroPriorityClassName != "hero-critical" {
					t.Errorf("HeroPriorityClassName = %q", c.HeroPriorityClassName)
				}
			},
		},
		{
			name: "weights and labels via file",
			yaml: "weights:\n  priority: 0.6\n  podCount: 0.3\n  runtime: 0.1\n" +
				"runtimeHalfLife: 3h\nstuckDetection: reason\n" +
				"nonBlockingPodLabels:\n  app: hpc-verification\n",
			want: func(t *testing.T, c Config) {
				if c.Weights != (Weights{Priority: 0.6, PodCount: 0.3, Runtime: 0.1}) {
					t.Errorf("Weights = %+v", c.Weights)
				}
				if c.RuntimeHalfLife.Duration != 3*time.Hour {
					t.Errorf("RuntimeHalfLife = %v", c.RuntimeHalfLife)
				}
				if c.StuckDetection != DetectionReason {
					t.Errorf("StuckDetection = %q", c.StuckDetection)
				}
				if c.NonBlockingPodLabels["app"] != "hpc-verification" {
					t.Errorf("NonBlockingPodLabels = %v", c.NonBlockingPodLabels)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := loadArgs(t, "--config", writeConfig(t, tc.yaml))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			tc.want(t, cfg)
		})
	}
}

// TestLoadWithCallerFlags reproduces the production wiring: the caller
// (cmd/main.go) registers its own flags — manager, logging — on the same
// FlagSet before Load, and the command line combines them with --config.
// A regression here crashloops the deployed controller on startup
// ("flag provided but not defined"), which is exactly how it was found.
func TestLoadWithCallerFlags(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var metricsAddr string
	fs.StringVar(&metricsAddr, "metrics-bind-address", "0", "")

	path := writeConfig(t, "taintKey: file.example.com/taint\ndrainTimeout: 10m\n")
	cfg, err := Load(fs, []string{
		"--config=" + path,
		"--metrics-bind-address=:8080",
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if metricsAddr != ":8080" {
		t.Errorf("caller flag = %q, want :8080", metricsAddr)
	}
	if cfg.TaintKey != "file.example.com/taint" {
		t.Errorf("TaintKey = %q, want file value", cfg.TaintKey)
	}
	if cfg.DrainTimeout.Duration != 10*time.Minute {
		t.Errorf("DrainTimeout = %v, want file value 10m", cfg.DrainTimeout.Duration)
	}
}

func TestValidationFailures(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(c *Config)
		wantSub string
	}{
		{
			name:    "weights do not sum to 1",
			mutate:  func(c *Config) { c.Weights = Weights{Priority: 0.7, PodCount: 0.2, Runtime: 0.2} },
			wantSub: "weights must sum to 1",
		},
		{
			name:    "negative weight",
			mutate:  func(c *Config) { c.Weights = Weights{Priority: 1.2, PodCount: -0.1, Runtime: -0.1} },
			wantSub: "non-negative",
		},
		{
			name:    "zero drain timeout",
			mutate:  func(c *Config) { c.DrainTimeout.Duration = 0 },
			wantSub: "drainTimeout must be positive",
		},
		{
			name:    "zero half life",
			mutate:  func(c *Config) { c.RuntimeHalfLife.Duration = 0 },
			wantSub: "runtimeHalfLife must be positive",
		},
		{
			name:    "bad taint key",
			mutate:  func(c *Config) { c.TaintKey = "not a key!" },
			wantSub: "taintKey",
		},
		{
			name:    "empty priority class",
			mutate:  func(c *Config) { c.HeroPriorityClassName = "" },
			wantSub: "heroPriorityClassName",
		},
		{
			name:    "cross-CQ multiplier below 1",
			mutate:  func(c *Config) { c.CrossCQMultiplier = 0.5 },
			wantSub: "crossCQMultiplier",
		},
		{
			name:    "unknown detection mode",
			mutate:  func(c *Config) { c.StuckDetection = "guess" },
			wantSub: "stuckDetection",
		},
		{
			name:    "bad non-blocking label value",
			mutate:  func(c *Config) { c.NonBlockingPodLabels = map[string]string{"app": "bad value!"} },
			wantSub: "nonBlockingPodLabels",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Default()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("Validate() = nil, want error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not mention %q", err, tc.wantSub)
			}
		})
	}
}

func TestLoadRejectsInvalid(t *testing.T) {
	if _, err := loadArgs(t, "--config", writeConfig(t, "weights:\n  priority: 0.9\n")); err == nil {
		t.Error("Load accepted weights that do not sum to 1")
	}
	if _, err := loadArgs(t, "--config", writeConfig(t, "stuckDetection: nope\n")); err == nil {
		t.Error("Load accepted unknown detection mode")
	}
}

func TestLoadRejectsUnknownYAMLField(t *testing.T) {
	path := writeConfig(t, "taintKey: example.com/ok\nnotAKnob: true\n")
	if _, err := loadArgs(t, "--config", path); err == nil {
		t.Error("Load accepted YAML with unknown field; UnmarshalStrict should reject typos")
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := loadArgs(t, "--config", "/does/not/exist.yaml"); err == nil {
		t.Error("Load accepted missing config file")
	}
}

func TestLoadRejectsBadLabelValue(t *testing.T) {
	if _, err := loadArgs(t, "--config", writeConfig(t, "nonBlockingPodLabels:\n  app: \"bad value!\"\n")); err == nil {
		t.Error("accepted invalid label value")
	}
}

func TestStuckAfterValidation(t *testing.T) {
	c := Default()
	if c.StuckAfter.Duration != 2*time.Minute {
		t.Fatalf("default StuckAfter = %v, want 2m", c.StuckAfter.Duration)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
	c.StuckAfter.Duration = 0 // zero disables the debounce
	if err := c.Validate(); err != nil {
		t.Errorf("stuckAfter=0 rejected: %v", err)
	}
	c.StuckAfter.Duration = -time.Second
	if err := c.Validate(); err == nil {
		t.Error("negative stuckAfter accepted")
	}
}
