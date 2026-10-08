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

package cmds

import (
	"encoding/json"
	"fmt"

	"kubeops.dev/dr-controlplane/pkg/leases"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func newCmdSwitchover() *cobra.Command {
	var (
		kubeconfig string
		namespace  = leases.DefaultNamespace
		group      string
		to         string
	)
	cmd := &cobra.Command{
		Use:               "switchover --to <dc> [--group <group>]",
		Short:             "Request a planned coordinated handoff of a scope's primary to another data center",
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if to == "" {
				return fmt.Errorf("--to is required")
			}
			scope := leases.GlobalScope
			if group != "" {
				scope = leases.GroupScope(group)
			}
			name := scope.PrimaryLeaseName()

			cs, err := coreClient(kubeconfig)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cur, err := cs.CoordinationV1().Leases(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("getting lease %s: %w", name, err)
			}
			if members := cur.Annotations[leases.AnnMemberDCs]; !leases.ContainsMember(members, to) {
				return fmt.Errorf("data center %q is not a Member of scope %s (members: %s); only Members can become primary", to, scope.String(), orDash(members))
			}

			patch := map[string]any{"metadata": map[string]any{"annotations": map[string]string{leases.AnnHandoffTo: to}}}
			raw, err := json.Marshal(patch)
			if err != nil {
				return err
			}
			if _, err := cs.CoordinationV1().Leases(namespace).Patch(ctx, name, types.MergePatchType, raw, metav1.PatchOptions{}); err != nil {
				return fmt.Errorf("patching lease %s: %w", name, err)
			}
			fmt.Printf("requested coordinated handoff of %s to %q. The current holder will release and non target candidates will pause so %q acquires.\n", scope.String(), to, to)
			return nil
		},
	}
	fs := cmd.Flags()
	fs.StringVar(&kubeconfig, "kubeconfig", kubeconfig, "Path to the coordination control plane kubeconfig. Empty means in cluster.")
	fs.StringVar(&namespace, "namespace", namespace, "Namespace holding the coordination Leases.")
	fs.StringVar(&group, "group", group, "Workload group for a Group scoped trigger. Empty selects the global scope.")
	fs.StringVar(&to, "to", to, "Target data center to hand the primary role to (must be a Member).")
	return cmd
}
