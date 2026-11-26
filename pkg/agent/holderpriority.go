/*
Copyright AppsCode Inc. and Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package agent

import (
	"context"
	"errors"
	"sync"
	"time"

	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

// Holder priority after a shared outage.
//
// The primary DC Lease moves when it expires. That is right when the holder DC
// is really gone, but an outage of the hub itself (an etcd brownout, a control
// plane restart) stops EVERY DC from renewing. If it lasts longer than the Lease
// duration, the Lease is expired for everyone when the hub returns, and plain
// leader election hands it to whichever agent retries first: a coin flip that
// can fail a healthy primary DC over for no reason.
//
// The guard: a NON-holder may take an expired Lease over from another DC only
// after its own hub writes (the DC health Lease renewals) have succeeded without
// a break for HolderPriorityWindow. After a shared outage every DC's streak
// starts again at the same moment, and the holder renews at once (its renewals
// are never gated), so it keeps the Lease. In a real DC loss the survivor's hub
// writes never broke, its streak is long, and it acquires at the usual Lease
// expiry with no added delay.
//
// The guard never blocks: renewals by the holder, acquiring a Lease that has no
// holder (a fresh Lease or a coordinated handoff release), and Create are all
// passed straight through.

// errTakeoverDeferred is returned by the lock when a takeover is deferred.
var errTakeoverDeferred = errors.New("takeover of the expired primary DC Lease deferred: this DC's own hub writes have not been healthy for the holder priority window yet")

// healthStreakBreak is how long a gap between two successful health renewals
// may be before the streak counts as broken. Three renew intervals: one or two
// failed renewals in a row (a blip) keep the streak, a real outage breaks it.
func (a *Agent) healthStreakBreak() time.Duration {
	return 3 * a.opts.HealthRenewInterval
}

// noteHealthRenewed records a successful health Lease renewal and maintains the
// streak of unbroken hub writes the holder priority guard reads.
func (a *Agent) noteHealthRenewed(now time.Time) {
	prev := a.lastRenewOK.Load()
	if prev == 0 || now.Sub(time.Unix(0, prev)) > a.healthStreakBreak() {
		a.healthStreakStart.Store(now.UnixNano())
	}
	a.lastRenewOK.Store(now.UnixNano())
	a.noteHealthOK()
}

// resetHealthStreak forgets the streak. Called when this replica stops being
// the writer: it no longer renews, so it has no evidence of hub health.
func (a *Agent) resetHealthStreak() {
	a.lastRenewOK.Store(0)
	a.healthStreakStart.Store(0)
}

// mayTakeOver reports whether this DC's hub writes have been healthy, without a
// break, for at least HolderPriorityWindow.
func (a *Agent) mayTakeOver(now time.Time) bool {
	window := a.opts.HolderPriorityWindow
	if window <= 0 {
		return true
	}
	last, start := a.lastRenewOK.Load(), a.healthStreakStart.Load()
	if last == 0 || start == 0 {
		return false
	}
	if now.Sub(time.Unix(0, last)) > a.healthStreakBreak() {
		return false
	}
	return now.Sub(time.Unix(0, start)) >= window
}

// holderPriorityLock wraps a scope's Lease lock and applies the guard to every
// Update that would change the holder from another DC to this one.
type holderPriorityLock struct {
	resourcelock.Interface
	a     *Agent
	scope string

	mu         sync.Mutex
	lastHolder string // holder in the last record read or written
}

func (l *holderPriorityLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	rec, raw, err := l.Interface.Get(ctx)
	if err == nil && rec != nil {
		l.mu.Lock()
		l.lastHolder = rec.HolderIdentity
		l.mu.Unlock()
	}
	return rec, raw, err
}

func (l *holderPriorityLock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	l.mu.Lock()
	prev := l.lastHolder
	l.mu.Unlock()
	if prev != "" && prev != ler.HolderIdentity && !l.a.mayTakeOver(time.Now()) {
		if l.a.metrics != nil {
			l.a.metrics.TakeoversDeferred.WithLabelValues(l.scope).Inc()
		}
		klog.InfoS("deferring takeover of the expired primary DC Lease to give the holder the first chance to renew",
			"dcdr.scope", l.scope, "dcdr.leaseHolder", prev, "dcdr.dc", ler.HolderIdentity, "window", l.a.opts.HolderPriorityWindow)
		return errTakeoverDeferred
	}
	if err := l.Interface.Update(ctx, ler); err != nil {
		return err
	}
	l.mu.Lock()
	l.lastHolder = ler.HolderIdentity
	l.mu.Unlock()
	return nil
}

func (l *holderPriorityLock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	if err := l.Interface.Create(ctx, ler); err != nil {
		return err
	}
	l.mu.Lock()
	l.lastHolder = ler.HolderIdentity
	l.mu.Unlock()
	return nil
}
