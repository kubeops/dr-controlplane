/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Free Trial License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Free-Trial-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package agent

import (
	"context"
	"time"

	"open-cluster-management.io/dr-controlplane/pkg/leases"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// The observation watchdog exists because the marker renewTime is only as alive
// as this agent's view of the Leases. The fence contract makes a frozen view
// indistinguishable from a dead control plane: markers stop restamping, the
// 30s fence TTL expires, and every DC (including the one the quorum still
// trusts) holds its databases read only. Observed live on 2026-08-07: both
// agents' Lease informers stopped delivering events at 04:52:38 while the
// leaderelection writes kept flowing on the same connection pool, the markers
// froze 13+ minutes behind the actual Lease renewTime, and every coordinator
// on the (healthy, quorum-trusted) active DC refused to run a primary. The
// watch died under client-side throttling; nothing noticed, nothing recovered.
//
// The watchdog turns that silent failure mode into a self-healing one:
//
//  1. Every ObservationCheckInterval it Lists the Leases with the aux client
//     (its own rate limiter, so elector floods cannot starve it) and compares
//     each primary Lease's actual renewTime with the last observed one.
//  2. Any Lease whose actual renewTime is ahead of the observed one by more
//     than ObservationStaleAfter is proof the informer has fallen behind (a
//     renewal happened that was never delivered). Every listed Lease is fed
//     through the same reconcile path an informer event would take, which
//     restores marker freshness immediately, well inside the fence TTL.
//  3. Staleness proven on ObservationRebuildStrikes consecutive checks means
//     the watch is dead, not merely late: the informer is rebuilt in place.
//
// Silence alone never triggers anything: an idle namespace has no renewals, so
// actual == observed and the check passes. Only drift, actual evidence that an
// update was missed, counts. The List is one request per interval against a
// namespace-scoped set of Leases, cheap at any scope count.
const (
	// ObservationCheckInterval is how often the watchdog compares the informer's
	// view against a direct List. It must be well under the pg-coordinator fence
	// TTL (30s) so a repair lands before the fence trips.
	ObservationCheckInterval = 10 * time.Second

	// ObservationStaleAfter is how far the actual renewTime may run ahead of the
	// observed one before the observation is declared stale. Held Leases renew
	// every RetryPeriod (2s), so 15s of drift is many missed deliveries, while
	// ordinary informer latency never approaches it.
	ObservationStaleAfter = 15 * time.Second

	// ObservationRebuildStrikes is how many consecutive stale checks it takes to
	// rebuild the informer. The List fallback keeps markers fresh in the
	// meantime, so the rebuild is deliberately unhurried.
	ObservationRebuildStrikes = 3
)

// runObservationWatchdog keeps the agent's Lease observation honest. It runs for
// the life of the agent; failures inside a check never take the agent down.
func (a *Agent) runObservationWatchdog(ctx context.Context) {
	klog.InfoS("lease observation watchdog running", "dcdr.dc", a.opts.DCName,
		"interval", ObservationCheckInterval.String(), "staleAfter", ObservationStaleAfter.String())
	ticker := time.NewTicker(ObservationCheckInterval)
	defer ticker.Stop()

	strikes := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stale, err := a.checkObservation(ctx)
			if err != nil {
				// Cannot List: the control plane is unreachable for the aux client
				// too. That is the health renewer's territory (its Lease goes
				// stale and the fence fails closed, which is correct); the
				// watchdog only judges the informer against a reachable server.
				klog.V(2).ErrorS(err, "observation check skipped: cannot list Leases")
				continue
			}
			if !stale {
				strikes = 0
				continue
			}
			strikes++
			if a.metrics != nil {
				a.metrics.ObservationRepairs.Inc()
			}
			if strikes < ObservationRebuildStrikes {
				continue
			}
			strikes = 0
			klog.ErrorS(nil, "lease informer proven stale on consecutive checks; rebuilding it",
				"dcdr.dc", a.opts.DCName, "strikes", ObservationRebuildStrikes)
			if a.metrics != nil {
				a.metrics.InformerRebuilds.Inc()
			}
			if err := a.startInformer(ctx); err != nil {
				klog.ErrorS(err, "failed to rebuild the lease informer; will keep repairing via List")
			}
		}
	}
}

// checkObservation Lists the Leases and repairs any proven-stale observation by
// running the ordinary reconcile path over the listed state. It reports whether
// staleness was proven.
func (a *Agent) checkObservation(ctx context.Context) (bool, error) {
	ls, err := a.aux.CoordinationV1().Leases(a.opts.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}

	stale := false
	for i := range ls.Items {
		l := &ls.Items[i]
		if !leases.IsPrimaryLeaseName(l.Name) || l.Spec.RenewTime == nil {
			continue
		}
		actual := l.Spec.RenewTime.Time
		a.mu.Lock()
		observed, seen := a.holders[l.Name]
		a.mu.Unlock()
		if seen && actual.Sub(observed.renew) <= ObservationStaleAfter {
			continue
		}
		if !seen {
			// A Lease the informer never delivered at all. Right after startup
			// that is ordinary latency; past it, it is the same missed-delivery
			// defect as drift. Either way reconciling from a listed Lease is
			// safe (it IS observed state, see the A43(c) comment on reconcile).
			klog.V(2).InfoS("observation check: lease never observed via informer, reconciling from list", "lease", l.Name)
		} else {
			stale = true
			klog.ErrorS(nil, "lease observation is stale, repairing from direct list",
				"lease", l.Name, "observedRenew", observed.renew.UTC().Format(time.RFC3339),
				"actualRenew", actual.UTC().Format(time.RFC3339))
		}
		a.reconcile(l)
	}
	return stale, nil
}
