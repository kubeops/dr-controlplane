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
	"time"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// Active DC marker contract. The projector writes it, the pg-coordinator fence
// reads it. Both sides must agree on these keys and on the ConfigMap name, which
// is the scope's primary Lease name (for example primary-dc for the global scope).
//
//	ConfigMap <primaryLeaseName> in MarkerNamespace
//	  data.activeDC  = the DC the quorum currently trusts as primary
//	  data.renewTime = RFC3339, the observed primary DC Lease renewTime
const (
	MarkerKeyActiveDC = "activeDC"
	MarkerKeyRenew    = "renewTime"
	// MarkerKeyQuiesce names the DC whose primary must hold read only for a planned
	// switchover. Empty in steady state. The active DC's coordinator reads it.
	MarkerKeyQuiesce = "quiesce"

	markerManagedByLabel = "app.kubernetes.io/managed-by"
	markerManagedByValue = "dr-controlplane-agent"
)

// runProjector mirrors each scope's primary DC holder into the local spoke as a
// marker ConfigMap, refreshed on an interval. A healthy agent restamps a fresh
// renewTime; a partitioned agent stops seeing Lease updates so the value freezes
// and the fence trips. Projection failures never take down the agent. On the
// same tick it also reconciles the break glass override-hold Lease annotation
// against this DC's local override ConfigMap (see override.go, A43(c)), and
// refreshes this DC's cached standby-hold state against its local standby-hold
// ConfigMap (see standbyhold.go, A44).
//
// Runs only on the writer replica (writer.go); all spoke reads go through the
// writer manager's cached client.
func (a *Agent) runProjector(ctx context.Context) {
	interval := a.opts.MarkerRefreshInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	klog.InfoS("active DC marker projector running", "dcdr.dc", a.opts.DCName, "namespace", a.opts.MarkerNamespace, "interval", interval.String())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.projectMarkers(ctx)
			a.reconcileOverrides(ctx)
			a.reconcileStandbyHold(ctx)
		}
	}
}

func (a *Agent) projectMarkers(ctx context.Context) {
	a.mu.Lock()
	snapshot := make(map[string]markerState, len(a.holders))
	for k, v := range a.holders {
		snapshot[k] = v
	}
	a.mu.Unlock()

	for name, st := range snapshot {
		if err := a.upsertMarker(ctx, name, st); err != nil {
			klog.ErrorS(err, "failed to project active DC marker", "dcdr.dc", a.opts.DCName, "dcdr.scope", name)
		}
	}
}

func (a *Agent) upsertMarker(ctx context.Context, name string, st markerState) error {
	renew := ""
	if !st.renew.IsZero() {
		renew = st.renew.UTC().Format(time.RFC3339)
	}
	data := map[string]string{
		MarkerKeyActiveDC: st.dc,
		MarkerKeyRenew:    renew,
		MarkerKeyQuiesce:  st.quiesce,
	}
	var cur core.ConfigMap
	err := a.spoke.Get(ctx, types.NamespacedName{Namespace: a.opts.MarkerNamespace, Name: name}, &cur)
	if apierrors.IsNotFound(err) {
		cm := &core.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: a.opts.MarkerNamespace,
				Labels:    map[string]string{markerManagedByLabel: markerManagedByValue},
			},
			Data: data,
		}
		return a.spoke.Create(ctx, cm)
	}
	if err != nil {
		return err
	}
	// Monotonic guard: never replace a marker with an OLDER renewTime. The fence
	// fails closed on staleness, so the only harmful write a racing or lagging
	// writer could make is one that ages the marker; refusing it makes even a
	// split-leadership instant harmless. Holder changes are safe under this rule
	// because moving a Lease always advances its renewTime.
	if curRenew, e := time.Parse(time.RFC3339, cur.Data[MarkerKeyRenew]); e == nil && renew != "" {
		if newRenew, e2 := time.Parse(time.RFC3339, renew); e2 == nil && newRenew.Before(curRenew) {
			klog.V(3).InfoS("skipping marker write with an older renewTime than the current marker",
				"dcdr.scope", name, "current", cur.Data[MarkerKeyRenew], "incoming", renew)
			return nil
		}
	}
	if cur.Data[MarkerKeyActiveDC] == data[MarkerKeyActiveDC] &&
		cur.Data[MarkerKeyRenew] == data[MarkerKeyRenew] &&
		cur.Data[MarkerKeyQuiesce] == data[MarkerKeyQuiesce] {
		return nil
	}
	cur.Data = data
	return a.spoke.Update(ctx, &cur)
}
