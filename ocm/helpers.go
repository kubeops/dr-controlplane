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
	"context"
	"encoding/base64"
	"regexp"
	"time"

	"gomodules.xyz/cert/certstore"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"open-cluster-management.io/addon-framework/pkg/addonfactory"
	"open-cluster-management.io/api/addon/v1alpha1"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
)

// agentInstallNamespace is the WithAgentInstallNamespace hook. The
// ManagedClusterAddOn CRD defaults Spec.InstallNamespace to
// open-cluster-management-agent-addon, so the addon-framework built in namespace
// can never be relied on to be the namespace that actually holds the coordination
// credential. Returning the configured namespace here overrides the built in one
// for the Helm release namespace, the addonInstallNamespace built in value, and
// the registration namespace at once.
func agentInstallNamespace(opts AgentOptions) func(addon *v1alpha1.ManagedClusterAddOn) (string, error) {
	return func(_ *v1alpha1.ManagedClusterAddOn) (string, error) {
		return opts.InstallNamespace, nil
	}
}

// getValues injects per cluster values into the embedded agent chart: the
// ManagedCluster name becomes the agent's --dc-name, and every agent tunable
// (install namespace, namespace ownership, coordination Secret, image, pull
// secrets, health and election timings) comes from the addon manager's own flags
// so nothing is hardcoded per cluster in the chart.
//
// restConfig and cs are threaded through for the follow up CSR work that will
// replace coord-kubeconfig; they are not consumed by the scaffold values.
func getValues(opts AgentOptions, restConfig *rest.Config, _ *certstore.CertStore) addonfactory.GetValuesFunc {
	return func(cluster *clusterv1.ManagedCluster, _ *v1alpha1.ManagedClusterAddOn) (addonfactory.Values, error) {
		pullSecrets := make([]interface{}, 0, len(opts.ImagePullSecrets))
		for _, name := range opts.ImagePullSecrets {
			if name == "" {
				continue
			}
			pullSecrets = append(pullSecrets, map[string]interface{}{"name": name})
		}

		values := addonfactory.Values{
			// clusterName drives the agent Deployment's --dc-name flag.
			"clusterName": cluster.Name,
			// namespace must match the effective addon install namespace, which
			// WithAgentInstallNamespace pins to opts.InstallNamespace.
			"namespace":       opts.InstallNamespace,
			"createNamespace": opts.CreateNamespace,
			"image": map[string]interface{}{
				"repository": opts.ImageRepository,
				"tag":        opts.ImageTag,
			},
			"imagePullSecrets": pullSecrets,
			"replicas":         opts.Replicas,
			"agent": map[string]interface{}{
				"coordKubeconfigSecret": opts.CoordKubeconfigSecret,
				// Ship the coordination credential itself so the agent can authenticate
				// without anyone copying a Secret to each spoke by hand. Empty when the
				// copy is disabled or the source cannot be read, in which case the chart
				// renders no Secret and an operator supplied one is used as before.
				"coordKubeconfigData": coordKubeconfigData(opts, restConfig),
				"health": map[string]interface{}{
					"leaseDurationSeconds": seconds(opts.HealthLeaseDuration),
					"renewIntervalSeconds": seconds(opts.HealthRenewInterval),
				},
				"election": map[string]interface{}{
					"leaseDurationSeconds": seconds(opts.ElectionLeaseDuration),
					"renewDeadlineSeconds": seconds(opts.ElectionRenewDeadline),
					"retryPeriodSeconds":   seconds(opts.ElectionRetryPeriod),
				},
			},
		}
		return values, nil
	}
}

// coordKubeconfigData reads the coordination control plane kubeconfig from the hub and
// rewrites its server URL to the externally reachable endpoint, returning it base64 encoded
// for the agent chart to render as a Secret on every managed cluster.
//
// This removes the manual credential fan-out. The control plane mints its own CAs into its
// data directory, so any rebuild or data-dir loss produces a NEW CA and every previously
// distributed kubeconfig stops validating ("x509: certificate signed by unknown authority").
// Recovering meant re-extracting the kubeconfig and applying it to every spoke by hand.
// Because the addon re-renders when the source Secret changes, the new credential now
// reaches every managed cluster on its own; the agent's liveness probe restarts it so the
// rotated file is actually re-read.
//
// Fail SOFT: any problem returns "" and the chart simply renders no Secret, leaving whatever
// an operator provisioned in place. A broken copy must never take down a working agent.
func coordKubeconfigData(opts AgentOptions, restConfig *rest.Config) string {
	if opts.CoordExternalEndpoint == "" || opts.CoordKubeconfigSourceSecret == "" || restConfig == nil {
		return ""
	}
	cs, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		klog.ErrorS(err, "cannot build a hub client to copy the coordination kubeconfig")
		return ""
	}
	sec, err := cs.CoreV1().Secrets(opts.InstallNamespace).Get(context.TODO(), opts.CoordKubeconfigSourceSecret, metav1.GetOptions{})
	if err != nil {
		klog.ErrorS(err, "cannot read the coordination kubeconfig Secret; agents will need one provisioned out of band",
			"namespace", opts.InstallNamespace, "secret", opts.CoordKubeconfigSourceSecret)
		return ""
	}
	raw, ok := sec.Data["kubeconfig"]
	if !ok || len(raw) == 0 {
		klog.ErrorS(nil, "coordination kubeconfig Secret has no kubeconfig key", "secret", opts.CoordKubeconfigSourceSecret)
		return ""
	}
	// The in-cluster kubeconfig points at a Service URL that does not resolve from another
	// cluster; agents must reach the external endpoint instead.
	rewritten := serverURLRe.ReplaceAll(raw, []byte("server: "+opts.CoordExternalEndpoint))
	return base64.StdEncoding.EncodeToString(rewritten)
}

var serverURLRe = regexp.MustCompile(`server:\s*https?://[^\s]+`)

// seconds renders a duration as whole seconds; the chart appends the "s" suffix.
func seconds(d time.Duration) int {
	return int(d / time.Second)
}
