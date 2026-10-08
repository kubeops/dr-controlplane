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
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"kubeops.dev/dr-controlplane/pkg/leases"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newCmdStatus() *cobra.Command {
	var (
		kubeconfig string
		namespace  = leases.DefaultNamespace
	)
	cmd := &cobra.Command{
		Use:               "status",
		Short:             "Show the failover scopes, their Members, the current primary, and per DC health",
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cs, err := coreClient(kubeconfig)
			if err != nil {
				return err
			}
			list, err := cs.CoordinationV1().Leases(namespace).List(cmd.Context(), metav1.ListOptions{})
			if err != nil {
				return err
			}
			items := list.Items
			sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })

			w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(w, "SCOPE\tLEASE\tMEMBERS\tPRIMARY\tHANDOFF\tRENEWED")
			for i := range items {
				l := &items[i]
				if !leases.IsPrimaryLeaseName(l.Name) {
					continue
				}
				scope, _ := leases.ScopeFromPrimaryLeaseName(l.Name)
				_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s ago\n",
					scope.String(), l.Name,
					orDash(l.Annotations[leases.AnnMemberDCs]),
					leaseHolder(l),
					orDash(l.Annotations[leases.AnnHandoffTo]),
					leaseAge(l))
			}
			_, _ = fmt.Fprintln(w, "\tDC HEALTH\t\t\t\t")
			for i := range items {
				l := &items[i]
				if leases.IsPrimaryLeaseName(l.Name) {
					continue
				}
				_, _ = fmt.Fprintf(w, "%s\t%s\t\t%s\t\t%s ago\n", "health", l.Name, leaseHolder(l), leaseAge(l))
			}
			return w.Flush()
		},
	}
	cmd.Flags().StringVar(&kubeconfig, "kubeconfig", kubeconfig, "Path to the coordination control plane kubeconfig. Empty means in cluster.")
	cmd.Flags().StringVar(&namespace, "namespace", namespace, "Namespace holding the coordination Leases.")
	return cmd
}
