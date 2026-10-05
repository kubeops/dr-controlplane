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

import "testing"

func TestDesiredContend(t *testing.T) {
	cases := []struct {
		name           string
		isMember       bool
		handoffTo      string
		holder         string
		dc             string
		targetIsMember bool
		overrideHold   string
		standbyHold    bool
		followDC       string
		followIsMember bool
		want           bool
	}{
		{"arbiter or witness never contends", false, "", "dc-a", "dc-c", false, "", false, "", false, false},
		{"member contends normally", true, "", "dc-a", "dc-b", false, "", false, "", false, true},
		{"member is the handoff target, contends eagerly", true, "dc-b", "dc-a", "dc-b", true, "", false, "", false, true},
		{"non-holder member pauses during handoff to another member", true, "dc-a", "dc-b", "dc-c", true, "", false, "", false, false},
		{"current holder releases during handoff to another member", true, "dc-a", "dc-b", "dc-b", true, "", false, "", false, false},
		{"handoff complete, target holds, resume normal", true, "dc-a", "dc-a", "dc-b", true, "", false, "", false, true},
		{"non member ignores handoff entirely", false, "dc-c", "", "dc-c", true, "", false, "", false, false},
		{"member ignores handoff to a non-member target (no no-primary deadlock)", true, "dc-z", "dc-a", "dc-a", false, "", false, "", false, true},
		{"break glass: pinned DC keeps contending (holds/renews)", true, "", "dc-a", "dc-a", false, "dc-a", false, "", false, true},
		{"break glass: non-pinned member defers unconditionally", true, "", "dc-a", "dc-b", false, "dc-a", false, "", false, false},
		{"break glass: arbiter/witness still never contends", false, "", "dc-a", "dc-c", false, "dc-a", false, "", false, false},
		{"break glass overrides an in progress handoff to another member", true, "dc-b", "dc-a", "dc-b", true, "dc-a", false, "", false, false},
		{"break glass overrides a handoff whose target is the pinned DC itself", true, "dc-a", "dc-b", "dc-a", true, "dc-a", false, "", false, true},
		{"standby-hold: held member (not the holder) never contends even with an otherwise clean Lease", true, "", "dc-a", "dc-b", false, "", true, "", false, false},
		{"standby-hold: held member (not the holder) never contends even as the handoff target", true, "dc-b", "dc-a", "dc-b", true, "", true, "", false, false},
		{"standby-hold: ignored on the current holder, keeps contending/renewing (A45 correction)", true, "", "dc-a", "dc-a", false, "", true, "", false, true},
		{"standby-hold: arbiter/witness still never contends", false, "", "dc-a", "dc-c", false, "", true, "", false, false},
		{"standby-hold ignored on the current holder even when break glass also pins it to itself (A45)", true, "", "dc-a", "dc-a", false, "dc-a", true, "", false, true},
		{"standby-hold beats break glass on a non-holder DC (conflict, fail safe to standby)", true, "", "dc-b", "dc-a", false, "dc-a", true, "", false, false},
		{"standby-hold ignored on the current holder does not break an in progress handoff away from it", true, "dc-b", "dc-a", "dc-a", true, "", true, "", false, false},
		{"follow-dc: non-holder member other than the named DC defers", true, "", "dc-a", "dc-c", false, "", false, "dc-b", true, false},
		{"follow-dc: named DC contends (acquires the expired Lease)", true, "", "dc-a", "dc-b", false, "", false, "dc-b", true, true},
		{"follow-dc: current holder keeps renewing, never forced out", true, "", "dc-a", "dc-a", false, "", false, "dc-b", true, true},
		{"follow-dc: named DC still pauses for a handoff to another member", true, "dc-c", "dc-a", "dc-b", true, "", false, "dc-b", true, false},
		{"follow-dc: explicit handoff target contends even when not the followed DC", true, "dc-c", "dc-a", "dc-c", true, "", false, "dc-b", true, true},
		{"follow-dc: non-member value is ignored (no no-primary deadlock)", true, "", "dc-a", "dc-c", false, "", false, "dc-z", false, true},
		{"follow-dc: break glass pin still wins", true, "", "dc-a", "dc-c", false, "dc-c", false, "dc-b", true, true},
		{"follow-dc: standby-hold still wins on a non-holder", true, "", "dc-a", "dc-b", false, "", true, "dc-b", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := desiredContend(tc.isMember, tc.handoffTo, tc.holder, tc.dc, tc.targetIsMember, tc.overrideHold, tc.standbyHold, tc.followDC, tc.followIsMember); got != tc.want {
				t.Fatalf("desiredContend(member=%v, handoffTo=%q, holder=%q, dc=%q, targetIsMember=%v, overrideHold=%q, standbyHold=%v, followDC=%q, followIsMember=%v) = %v, want %v",
					tc.isMember, tc.handoffTo, tc.holder, tc.dc, tc.targetIsMember, tc.overrideHold, tc.standbyHold, tc.followDC, tc.followIsMember, got, tc.want)
			}
		})
	}
}
