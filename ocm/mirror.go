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

package ocm

import (
	"bytes"
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// coordKubeconfigMirrorInterval is how often the mirror is reconciled. Short enough that a
// rotated CA reaches the operator quickly, long enough to be free.
const coordKubeconfigMirrorInterval = 30 * time.Second

// runCoordKubeconfigMirror keeps a copy of the coordination kubeconfig in each configured
// hub namespace.
//
// The addon already ships this credential to every MANAGED CLUSTER, but the KubeDB operator
// mounts it from its own namespace on the hub, which no addon manifest reaches. That left
// one credential outside the automation: it had to be created by hand before the operator
// could talk to the control plane, and re-created every time the control plane minted a new
// CA. Observed live, this was the last manual step of a rebuild and the easiest to forget,
// because the operator does not crash without it: it just logs x509 forever and the DR
// status quietly stops updating.
//
// Only the payload is reconciled, and only when it differs, so this does not fight anyone
// editing unrelated fields and does not churn the API server. The operator still has to be
// restarted to pick up a rotated credential (it reads the file once at startup); that
// restart is the one remaining manual step and is out of this component's reach.
func runCoordKubeconfigMirror(ctx context.Context, opts AgentOptions, restConfig *rest.Config) {
	if len(opts.CoordKubeconfigMirrorNamespaces) == 0 || opts.CoordExternalEndpoint == "" {
		return
	}
	cs, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		klog.ErrorS(err, "cannot build a hub client for the coordination kubeconfig mirror")
		return
	}
	ticker := time.NewTicker(coordKubeconfigMirrorInterval)
	defer ticker.Stop()
	for {
		mirrorCoordKubeconfigOnce(ctx, cs, opts)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func mirrorCoordKubeconfigOnce(ctx context.Context, cs kubernetes.Interface, opts AgentOptions) {
	src, err := cs.CoreV1().Secrets(opts.InstallNamespace).Get(ctx, opts.CoordKubeconfigSourceSecret, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.V(2).ErrorS(err, "coordination kubeconfig mirror: cannot read the source Secret",
				"namespace", opts.InstallNamespace, "secret", opts.CoordKubeconfigSourceSecret)
		}
		return
	}
	raw, ok := src.Data["kubeconfig"]
	if !ok || len(raw) == 0 {
		return
	}
	want := serverURLRe.ReplaceAll(raw, []byte("server: "+opts.CoordExternalEndpoint))

	for _, ns := range opts.CoordKubeconfigMirrorNamespaces {
		if ns == "" {
			continue
		}
		cur, err := cs.CoreV1().Secrets(ns).Get(ctx, opts.CoordKubeconfigSecret, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			_, err = cs.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: opts.CoordKubeconfigSecret, Namespace: ns},
				Data:       map[string][]byte{"kubeconfig": want},
			}, metav1.CreateOptions{})
			if err == nil {
				klog.InfoS("mirrored the coordination kubeconfig", "namespace", ns, "secret", opts.CoordKubeconfigSecret)
			}
		case err == nil:
			if bytes.Equal(cur.Data["kubeconfig"], want) {
				continue // already current; do not churn
			}
			if cur.Data == nil {
				cur.Data = map[string][]byte{}
			}
			cur.Data["kubeconfig"] = want
			_, err = cs.CoreV1().Secrets(ns).Update(ctx, cur, metav1.UpdateOptions{})
			if err == nil {
				klog.InfoS("refreshed the mirrored coordination kubeconfig (the consumer must still be restarted to re-read it)",
					"namespace", ns, "secret", opts.CoordKubeconfigSecret)
			}
		}
		if err != nil {
			klog.ErrorS(err, "coordination kubeconfig mirror failed", "namespace", ns, "secret", opts.CoordKubeconfigSecret)
		}
	}
}
