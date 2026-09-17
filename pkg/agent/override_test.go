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

func TestOverrideDecision(t *testing.T) {
	cases := []struct {
		name       string
		active     bool
		lastHolder string
		dc         string
		current    string
		wantValue  string
		wantChange bool
	}{
		{"active on the last known primary, pin not yet set: pin it", true, "dc-a", "dc-a", "", "dc-a", true},
		{"active on the last known primary, already pinned: no-op", true, "dc-a", "dc-a", "dc-a", "dc-a", false},
		{"active on a spoke that was never the holder: refuse, leave current alone", true, "dc-b", "dc-a", "", "", false},
		{"active on a spoke that was never the holder, some other pin exists: leave it alone", true, "dc-b", "dc-a", "dc-c", "dc-c", false},
		{"cleared, this DC's own pin present: drop it", false, "dc-a", "dc-a", "dc-a", "", true},
		{"cleared, no pin present: no-op", false, "dc-a", "dc-a", "", "", false},
		{"cleared, another DC's pin present: never touch it", false, "dc-a", "dc-a", "dc-b", "dc-b", false},
		{"never active and never held: no-op", false, "", "dc-a", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotValue, gotChange := overrideDecision(tc.active, tc.lastHolder, tc.dc, tc.current)
			if gotValue != tc.wantValue || gotChange != tc.wantChange {
				t.Fatalf("overrideDecision(active=%v, lastHolder=%q, dc=%q, current=%q) = (%q, %v), want (%q, %v)",
					tc.active, tc.lastHolder, tc.dc, tc.current, gotValue, gotChange, tc.wantValue, tc.wantChange)
			}
		})
	}
}
