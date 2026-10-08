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

	"kubeops.dev/dr-controlplane/pkg/leases"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// runHealthRenewer continuously renews this DC's health Lease. A DC that loses
// the etcd majority cannot renew, and observers see the Lease go stale.
func (a *Agent) runHealthRenewer(ctx context.Context) {
	name := leases.HealthLeaseName(a.opts.DCName)
	ticker := time.NewTicker(a.opts.HealthRenewInterval)
	defer ticker.Stop()
	for {
		a.renewHealth(ctx, name)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *Agent) renewHealth(ctx context.Context, name string) {
	durSec := int32(a.opts.HealthLeaseDuration.Seconds())
	now := metav1.NewMicroTime(time.Now())
	// The aux client: the health Lease is the DC's liveness signal, so its renewals
	// must never queue behind elector traffic in a shared rate limiter. Observed
	// live: renewals starving in that queue restarted the agents 180+ times in one
	// night via the /healthz staleness probe.
	cl := a.aux.CoordinationV1().Leases(a.opts.Namespace)

	cur, err := cl.Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		l := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: a.opts.Namespace,
				Labels:    map[string]string{leases.LabelManagedBy: leases.ValueManagedBy},
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       ptr.To(a.opts.DCName),
				LeaseDurationSeconds: ptr.To(durSec),
				AcquireTime:          &now,
				RenewTime:            &now,
			},
		}
		_, err = cl.Create(ctx, l, metav1.CreateOptions{})
	case err == nil:
		cur.Spec.HolderIdentity = ptr.To(a.opts.DCName)
		cur.Spec.LeaseDurationSeconds = ptr.To(durSec)
		cur.Spec.RenewTime = &now
		_, err = cl.Update(ctx, cur, metav1.UpdateOptions{})
	}

	if err != nil {
		a.metrics.HealthRenewErrors.Inc()
		if credentialClassError(err) {
			// Only credential-class failures may fail the liveness probe: a
			// restart re-reads the mounted kubeconfig, which is the one thing
			// it can fix. Plain unreachability must never restart the writer
			// (observed live: the probe bounced the writer role between
			// replicas for an entire hub outage).
			a.noteAuthFailure()
			klog.ErrorS(err, "health Lease renewal failed with a credential-class error; the liveness probe will restart this pod to reload the kubeconfig", "dcdr.scope", name)
			return
		}
		klog.V(2).ErrorS(err, "health Lease renewal failed (hub unreachable or etcd majority lost; fail-closed fencing protects the databases)", "dcdr.scope", name)
		return
	}
	a.metrics.HealthRenewals.Inc()
	a.noteHealthOK()
}
