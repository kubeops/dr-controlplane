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
	"encoding/json"

	"open-cluster-management.io/dr-controlplane/pkg/leases"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// desiredContend decides whether this DC should actively contend for a scope's
// primary DC Lease right now, given its membership and any in progress
// coordinated handoff. handoffTargetIsMember reports whether handoffTo names a
// Member data center (one that can actually become primary).
//
//   - Arbiter and Witness DCs (not members) never contend.
//   - Normally a Member contends.
//   - During a handoff to another Member, a Member pauses so the target can
//     acquire. If this DC currently holds the Lease, pausing releases it
//     (ReleaseOnCancel).
//   - The handoff target contends eagerly. Once the target holds the Lease the
//     handoff is complete and everyone resumes normal contention.
//   - A handoff whose target is not a Member (a stale annotation, or a Member
//     removed from the set mid handoff) is ignored. Otherwise every Member would
//     pause for a target that can never acquire, leaving the scope with no
//     primary at all.
func desiredContend(isMember bool, handoffTo, holder, dc string, handoffTargetIsMember bool) bool {
	if !isMember {
		return false
	}
	if handoffTo == "" || !handoffTargetIsMember || handoffTo == dc {
		return true
	}
	if holder == handoffTo {
		return true
	}
	return false
}

func holderOf(l *coordinationv1.Lease) string {
	if l == nil || l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

// clearHandoff removes the handoff annotation once the target holds the Lease.
func (a *Agent) clearHandoff(ctx context.Context, name string) {
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				leases.AnnHandoffTo: nil,
			},
		},
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return
	}
	if _, err := a.cs.CoordinationV1().Leases(a.opts.Namespace).
		Patch(ctx, name, types.MergePatchType, raw, metav1.PatchOptions{}); err != nil {
		klog.V(3).ErrorS(err, "failed to clear handoff annotation", "dcdr.scope", name)
		return
	}
	klog.InfoS("coordinated handoff complete, cleared handoff annotation", "dcdr.scope", name, "dcdr.dc", a.opts.DCName)
}
