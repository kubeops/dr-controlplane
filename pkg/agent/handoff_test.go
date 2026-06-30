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
		want           bool
	}{
		{"arbiter never contends", false, "", "dc-a", "dc-c", false, false},
		{"member contends normally", true, "", "dc-a", "dc-b", false, true},
		{"member is the handoff target, contends eagerly", true, "dc-b", "dc-a", "dc-b", true, true},
		{"non-holder member pauses during handoff to another member", true, "dc-a", "dc-b", "dc-c", true, false},
		{"current holder releases during handoff to another member", true, "dc-a", "dc-b", "dc-b", true, false},
		{"handoff complete, target holds, resume normal", true, "dc-a", "dc-a", "dc-b", true, true},
		{"non member ignores handoff entirely", false, "dc-c", "", "dc-c", true, false},
		{"member ignores handoff to a non-member target (no no-primary deadlock)", true, "dc-z", "dc-a", "dc-a", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := desiredContend(tc.isMember, tc.handoffTo, tc.holder, tc.dc, tc.targetIsMember); got != tc.want {
				t.Fatalf("desiredContend(member=%v, handoffTo=%q, holder=%q, dc=%q, targetIsMember=%v) = %v, want %v",
					tc.isMember, tc.handoffTo, tc.holder, tc.dc, tc.targetIsMember, got, tc.want)
			}
		})
	}
}
