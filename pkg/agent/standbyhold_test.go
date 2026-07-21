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

func TestStandbyHoldTransitionLog(t *testing.T) {
	cases := []struct {
		name           string
		wasActive      bool
		active         bool
		isHolder       bool
		wantStepDown   bool
		wantHeld       bool
		wantCleared    bool
	}{
		{"newly held on the current holder: loud controlled step-down", false, true, true, true, false, false},
		{"newly held on a non-holder standby: ordinary held log", false, true, false, false, true, false},
		{"still held, no change: silent", true, true, false, false, false, false},
		{"still held on the holder, no change: silent (no repeat step-down spam)", true, true, true, false, false, false},
		{"newly cleared while it had been held on the holder: cleared log", true, false, true, false, false, true},
		{"newly cleared while it had been held on a standby: cleared log", true, false, false, false, false, true},
		{"never active: silent", false, false, false, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stepDown, held, cleared := standbyHoldTransitionLog(tc.wasActive, tc.active, tc.isHolder)
			if stepDown != tc.wantStepDown || held != tc.wantHeld || cleared != tc.wantCleared {
				t.Fatalf("standbyHoldTransitionLog(wasActive=%v, active=%v, isHolder=%v) = (%v, %v, %v), want (%v, %v, %v)",
					tc.wasActive, tc.active, tc.isHolder, stepDown, held, cleared, tc.wantStepDown, tc.wantHeld, tc.wantCleared)
			}
		})
	}
}

func TestStandbyHoldConflict(t *testing.T) {
	cases := []struct {
		name           string
		standbyActive  bool
		overrideActive bool
		want           bool
	}{
		{"neither present: no conflict", false, false, false},
		{"only standby-hold present: no conflict", true, false, false},
		{"only override present: no conflict", false, true, false},
		{"both present: conflict", true, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := standbyHoldConflict(tc.standbyActive, tc.overrideActive); got != tc.want {
				t.Fatalf("standbyHoldConflict(standbyActive=%v, overrideActive=%v) = %v, want %v",
					tc.standbyActive, tc.overrideActive, got, tc.want)
			}
		})
	}
}
