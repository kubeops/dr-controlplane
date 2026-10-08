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

package leases

import (
	"sort"
	"strings"
)

// FormatMemberDCs renders a member DC set into the AnnMemberDCs annotation value,
// sorted and de duplicated so the annotation is stable.
func FormatMemberDCs(members []string) string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(members))
	for _, m := range members {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		if _, ok := seen[m]; ok {
			continue
		}
		seen[m] = struct{}{}
		out = append(out, m)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// ParseMemberDCs parses an AnnMemberDCs annotation value into a set.
func ParseMemberDCs(v string) map[string]struct{} {
	set := map[string]struct{}{}
	for _, m := range strings.Split(v, ",") {
		m = strings.TrimSpace(m)
		if m != "" {
			set[m] = struct{}{}
		}
	}
	return set
}

// ContainsMember reports whether dc is in the AnnMemberDCs annotation value.
func ContainsMember(v, dc string) bool {
	_, ok := ParseMemberDCs(v)[dc]
	return ok
}
