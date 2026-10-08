package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"strings"
	"time"
)

// The canary wait closes the start-up window without an isolation gate
// (connector drivers D5, "Reachability"): kube-router let 5 of 12 new pods
// through before their policy applied, and those connections stayed open.
//
// A refusal alone proves nothing: with the network down or the orchestrator
// without endpoints (a node restart, k3d stopping and starting) every
// connect fails, policy or not. So each round probes every --allow target
// first, and a --deny refusal counts only in a round where every allow
// target answered. The deny and allow targets are the same Service (the
// orchestrator's API port and its lease exchange port), so "the exchange
// answers and the API port is refused" in one round can only mean the pod's
// policy is in force. Any other round resets every count. The driver's code
// starts after --consecutive such rounds in a row.
//
// Then every --expect target is waited for at most --expect-timeout, never
// fatally: an upstream that is down must not keep the pod from starting.
//
// Connections this container opens end when it exits. Only TCP connects are
// made; no byte is sent.

type canaryConfig struct {
	deny, allow, expect []string
	consecutive         int
	interval            time.Duration
	timeout             time.Duration
	expectTimeout       time.Duration
	dialTimeout         time.Duration
}

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(value string) error {
	if _, _, err := net.SplitHostPort(value); err != nil {
		return fmt.Errorf("%q is not HOST:PORT", value)
	}
	*s = append(*s, value)
	return nil
}

func parseCanaryFlags(args []string) (canaryConfig, error) {
	var deny, allow, expect stringList
	flags := flag.NewFlagSet("canary-wait", flag.ContinueOnError)
	flags.Var(&deny, "deny", "a HOST:PORT the pod must not reach (repeatable)")
	flags.Var(&allow, "allow", "a HOST:PORT the pod must reach (repeatable)")
	flags.Var(&expect, "expect", "a HOST:PORT the pod should reach (repeatable)")
	consecutive := flags.Int("consecutive", 3, "refusals in a row that prove the deny")
	interval := flags.Duration("interval", time.Second, "time between probes")
	timeout := flags.Duration("timeout", 2*time.Minute, "give up after this long")
	expectTimeout := flags.Duration("expect-timeout", 15*time.Second, "wait this long for --expect")
	dialTimeout := flags.Duration("dial-timeout", time.Second, "one connect attempt")
	if err := flags.Parse(args); err != nil {
		return canaryConfig{}, err
	}
	if flags.NArg() != 0 {
		return canaryConfig{}, fmt.Errorf("unexpected arguments %q", flags.Args())
	}
	if len(deny) == 0 || len(allow) == 0 {
		return canaryConfig{}, errors.New("at least one --deny and one --allow are required")
	}
	if *consecutive < 1 || *interval <= 0 || *timeout <= 0 || *dialTimeout <= 0 || *expectTimeout < 0 {
		return canaryConfig{}, errors.New("counts and durations must be positive")
	}
	return canaryConfig{
		deny:          deny,
		allow:         allow,
		expect:        expect,
		consecutive:   *consecutive,
		interval:      *interval,
		timeout:       *timeout,
		expectTimeout: *expectTimeout,
		dialTimeout:   *dialTimeout,
	}, nil
}

// probe reports whether a TCP connection to addr completed.
type probe func(addr string) bool

func systemProbe(timeout time.Duration) probe {
	return func(addr string) bool {
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}
}

type clock interface {
	Now() time.Time
	Sleep(time.Duration)
}

type systemClock struct{}

func (systemClock) Now() time.Time        { return time.Now() }
func (systemClock) Sleep(d time.Duration) { time.Sleep(d) }

func canaryWait(cfg canaryConfig, reach probe, c clock, logf func(string, ...any)) error {
	deadline := c.Now().Add(cfg.timeout)
	rounds := 0 // rounds in a row where every allow answered and every deny was refused
	for rounds < cfg.consecutive {
		var unreachable []string
		for _, target := range cfg.allow {
			if !reach(target) {
				unreachable = append(unreachable, target)
			}
		}
		if len(unreachable) > 0 {
			// Without an answer from the allowed targets a refusal could be
			// a network that is down, not a policy: nothing counts.
			if rounds > 0 {
				logf("%s stopped answering; counting again", strings.Join(unreachable, ", "))
			}
			rounds = 0
		} else {
			enforced := true
			for _, target := range cfg.deny {
				if reach(target) {
					logf("canary %s is still reachable: the policy is not enforced yet", target)
					enforced = false
				}
			}
			if enforced {
				rounds++
			} else {
				rounds = 0
			}
		}
		if rounds >= cfg.consecutive {
			break
		}
		if !c.Now().Before(deadline) {
			if len(unreachable) > 0 {
				return fmt.Errorf("%s stayed unreachable for %s: the pod's egress policy is not in force", strings.Join(unreachable, ", "), cfg.timeout)
			}
			return fmt.Errorf("no %d rounds in a row with the allowed targets answering and the canaries refused in %s: the default deny is not enforced", cfg.consecutive, cfg.timeout)
		}
		c.Sleep(cfg.interval)
	}
	logf("default deny enforced: %d rounds in a row with %s answering and %s refused", cfg.consecutive, strings.Join(cfg.allow, ", "), strings.Join(cfg.deny, ", "))
	expectDeadline := c.Now().Add(cfg.expectTimeout)
	pending := append([]string(nil), cfg.expect...)
	for len(pending) > 0 {
		var still []string
		for _, target := range pending {
			if !reach(target) {
				still = append(still, target)
			}
		}
		pending = still
		if len(pending) == 0 || !c.Now().Before(expectDeadline) {
			break
		}
		c.Sleep(cfg.interval)
	}
	if len(pending) > 0 {
		logf("upstream %s did not answer in %s; starting the driver anyway", strings.Join(pending, ", "), cfg.expectTimeout)
	}
	return nil
}
