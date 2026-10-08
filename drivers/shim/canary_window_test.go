package main

import (
	"testing"
	"time"
)

// Windows in which the canary must not pass without a policy (D5 re-review).

// timed answers per target from a function of the elapsed time.
func timed(c *fakeClock, start time.Time, f func(addr string, t time.Duration) bool) probe {
	return func(addr string) bool { return f(addr, c.now.Sub(start)) }
}

// The deny target is the exchange server's own canary listener; allow is the
// exchange. Both are sockets of one server.
const windowDeny, windowAllow = "10.43.0.20:8089", "10.43.0.20:8088"

func windowConfig() canaryConfig {
	c := config()
	c.deny = []string{windowDeny}
	c.allow = []string{windowAllow}
	c.expect = nil
	c.timeout = 120 * time.Second
	return c
}

// No NetworkPolicy anywhere while the orchestrator (one replica, Recreate)
// shuts down: preStop 15 s with every port up, then SIGTERM. The API port
// (8085) closes first and the exchange drains for up to 10 s; with 8085 as
// the deny target that gave three rounds of "exchange answers, canary
// refused" and the canary passed with no policy. The canary listener is the
// exchange server's own, so the drain closes both at once: no round counts.
func TestCanaryNeverPassesWhileTheExchangeServerShutsDown(t *testing.T) {
	c := &fakeClock{now: time.Unix(0, 0)}
	start := c.now
	reach := timed(c, start, func(addr string, e time.Duration) bool {
		switch {
		case e < 15*time.Second:
			return true // no policy: everything answers
		case e < 25*time.Second:
			return false // the exchange server stopped both listeners
		default:
			return false // no endpoints
		}
	})
	if err := canaryWait(windowConfig(), reach, c, silent); err == nil {
		t.Fatalf("PASSED with no policy at t=%s", c.now.Sub(start))
	}
}

// What the old deny target allowed: the same shutdown with the canary
// closing ten seconds before the exchange passes. This is why the canary
// must share the exchange's listener lifecycle.
func TestASeparatelyClosingCanaryWouldPassWithoutPolicy(t *testing.T) {
	c := &fakeClock{now: time.Unix(0, 0)}
	start := c.now
	reach := timed(c, start, func(addr string, e time.Duration) bool {
		switch {
		case e < 15*time.Second:
			return true
		case e < 25*time.Second:
			return addr == windowAllow
		default:
			return false
		}
	})
	if err := canaryWait(windowConfig(), reach, c, silent); err != nil {
		t.Fatalf("the window should fool the wait (it is the topology that must prevent it): %v", err)
	}
}

// The exchange flaps every other round, the canary always refused: never.
func TestCanaryNeverPassesOnAFlappingExchange(t *testing.T) {
	c := &fakeClock{now: time.Unix(0, 0)}
	n := 0
	reach := func(addr string) bool {
		if addr == windowAllow {
			n++
			return n%2 == 0
		}
		return false
	}
	if err := canaryWait(windowConfig(), reach, c, silent); err == nil {
		t.Fatal("passed with a flapping exchange")
	}
}

// Two good rounds, one with the canary reachable, then good: it passes only
// after three more.
func TestCanaryCountsAgainAfterTheCanaryWasReachable(t *testing.T) {
	var calls []string
	s := scripted{windowAllow: {true}, windowDeny: {false, false, true, false, false, false}}
	c := &fakeClock{now: time.Unix(0, 0)}
	if err := canaryWait(windowConfig(), s.probe(&calls), c, silent); err != nil {
		t.Fatal(err)
	}
	if got := c.now.Sub(time.Unix(0, 0)); got != 5*time.Second {
		t.Fatalf("passed after %s, want 5s (6 rounds)", got)
	}
}

// A window shorter than three rounds never passes.
func TestCanaryNeverPassesOnAShortWindow(t *testing.T) {
	c := &fakeClock{now: time.Unix(0, 0)}
	start := c.now
	reach := timed(c, start, func(addr string, e time.Duration) bool {
		if e >= 5*time.Second && e < 7*time.Second {
			return addr == windowAllow
		}
		return true
	})
	if err := canaryWait(windowConfig(), reach, c, silent); err == nil {
		t.Fatal("passed on a 2 s window")
	}
}
