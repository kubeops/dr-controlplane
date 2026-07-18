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

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
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
// and the fence trips. Projection failures never take down the agent.
func (a *Agent) runProjector(ctx context.Context) {
	interval := a.opts.MarkerRefreshInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	klog.InfoS("active DC marker projector running", "dcdr.dc", a.opts.DCName, "namespace", a.opts.MarkerNamespace, "interval", interval.String())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var spoke kubernetes.Interface
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if spoke == nil {
				// Build the spoke client lazily and retry on every tick. A transient
				// failure here must not disable projection for the agent's lifetime:
				// this DC could be elected active while its marker is never written,
				// and the consumer fence would then hold every leader read only.
				cfg, err := a.opts.SpokeRESTConfig()
				if err != nil {
					klog.ErrorS(err, "active DC marker projection waiting: no spoke client config")
					continue
				}
				s, err := kubernetes.NewForConfig(cfg)
				if err != nil {
					klog.ErrorS(err, "active DC marker projection waiting: cannot build spoke client")
					continue
				}
				spoke = s
				klog.InfoS("active DC marker projector spoke client ready")
			}
			a.projectMarkers(ctx, spoke)
		}
	}
}

func (a *Agent) projectMarkers(ctx context.Context, spoke kubernetes.Interface) {
	a.mu.Lock()
	snapshot := make(map[string]markerState, len(a.holders))
	for k, v := range a.holders {
		snapshot[k] = v
	}
	a.mu.Unlock()

	for name, st := range snapshot {
		if err := a.upsertMarker(ctx, spoke, name, st); err != nil {
			klog.ErrorS(err, "failed to project active DC marker", "dcdr.dc", a.opts.DCName, "dcdr.scope", name)
		}
	}
}

func (a *Agent) upsertMarker(ctx context.Context, spoke kubernetes.Interface, name string, st markerState) error {
	renew := ""
	if !st.renew.IsZero() {
		renew = st.renew.UTC().Format(time.RFC3339)
	}
	data := map[string]string{
		MarkerKeyActiveDC: st.dc,
		MarkerKeyRenew:    renew,
		MarkerKeyQuiesce:  st.quiesce,
	}
	cms := spoke.CoreV1().ConfigMaps(a.opts.MarkerNamespace)
	cur, err := cms.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		cm := &core.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: a.opts.MarkerNamespace,
				Labels:    map[string]string{markerManagedByLabel: markerManagedByValue},
			},
			Data: data,
		}
		_, err = cms.Create(ctx, cm, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if cur.Data[MarkerKeyActiveDC] == data[MarkerKeyActiveDC] &&
		cur.Data[MarkerKeyRenew] == data[MarkerKeyRenew] &&
		cur.Data[MarkerKeyQuiesce] == data[MarkerKeyQuiesce] {
		return nil
	}
	cur.Data = data
	_, err = cms.Update(ctx, cur, metav1.UpdateOptions{})
	return err
}
