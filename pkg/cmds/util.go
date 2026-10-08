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

package cmds

import (
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// restConfig builds a client config from a kubeconfig path, or in cluster when empty.
// The client-go default rate limiter (QPS=5/Burst=10) has starved DC-DR control loops
// twice (agent elector fleet 2026-08-07; conflict storm 2026-09-11), so every client this
// service builds gets an effectively-unthrottled limiter and the API server's own
// priority-and-fairness does the real protecting.
func restConfig(kubeconfig string) (*rest.Config, error) {
	var cfg *rest.Config
	var err error
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}
	cfg.QPS = 50000
	cfg.Burst = 50000
	return cfg, nil
}

// coreClient builds a Kubernetes clientset for the coordination control plane.
func coreClient(kubeconfig string) (kubernetes.Interface, error) {
	cfg, err := restConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

func leaseHolder(l *coordinationv1.Lease) string {
	if l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity == "" {
		return "<none>"
	}
	return *l.Spec.HolderIdentity
}

func leaseAge(l *coordinationv1.Lease) string {
	if l.Spec.RenewTime == nil {
		return "n/a"
	}
	return time.Since(l.Spec.RenewTime.Time).Truncate(time.Second).String()
}

func orDash(s string) string {
	if s == "" {
		return "<none>"
	}
	return s
}
