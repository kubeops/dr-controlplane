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

package agent

import "testing"

func TestStandbyHoldTransitionLog(t *testing.T) {
	cases := []struct {
		name        string
		wasActive   bool
		active      bool
		isHolder    bool
		wantHeld    bool
		wantCleared bool
	}{
		{"newly held on the current holder: no transition log here, standbyHoldIgnoredOnActive covers it", false, true, true, false, false},
		{"newly held on a non-holder standby: ordinary held log", false, true, false, true, false},
		{"still held, no change: silent", true, true, false, false, false},
		{"still held on the holder, no change: silent", true, true, true, false, false},
		{"newly cleared while it had been held on the holder: cleared log", true, false, true, false, true},
		{"newly cleared while it had been held on a standby: cleared log", true, false, false, false, true},
		{"never active: silent", false, false, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			held, cleared := standbyHoldTransitionLog(tc.wasActive, tc.active, tc.isHolder)
			if held != tc.wantHeld || cleared != tc.wantCleared {
				t.Fatalf("standbyHoldTransitionLog(wasActive=%v, active=%v, isHolder=%v) = (%v, %v), want (%v, %v)",
					tc.wasActive, tc.active, tc.isHolder, held, cleared, tc.wantHeld, tc.wantCleared)
			}
		})
	}
}

func TestStandbyHoldIgnoredOnActive(t *testing.T) {
	cases := []struct {
		name     string
		active   bool
		isHolder bool
		want     bool
	}{
		{"active on the current holder: ignored, loud log expected (A45)", true, true, true},
		{"active on a non-holder: takes effect, no ignored log", true, false, false},
		{"inactive on the holder: nothing to ignore", false, true, false},
		{"inactive on a non-holder: nothing to ignore", false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := standbyHoldIgnoredOnActive(tc.active, tc.isHolder); got != tc.want {
				t.Fatalf("standbyHoldIgnoredOnActive(active=%v, isHolder=%v) = %v, want %v",
					tc.active, tc.isHolder, got, tc.want)
			}
		})
	}
}

func TestStandbyHoldConflict(t *testing.T) {
	cases := []struct {
		name           string
		standbyActive  bool
		overrideActive bool
		isHolder       bool
		want           bool
	}{
		{"neither present: no conflict", false, false, false, false},
		{"only standby-hold present: no conflict", true, false, false, false},
		{"only override present: no conflict", false, true, false, false},
		{"both present, non-holder: conflict, standby-hold wins", true, true, false, true},
		{"both present, current holder: no conflict, standby-hold is ignored here (A45)", true, true, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := standbyHoldConflict(tc.standbyActive, tc.overrideActive, tc.isHolder); got != tc.want {
				t.Fatalf("standbyHoldConflict(standbyActive=%v, overrideActive=%v, isHolder=%v) = %v, want %v",
					tc.standbyActive, tc.overrideActive, tc.isHolder, got, tc.want)
			}
		})
	}
}
