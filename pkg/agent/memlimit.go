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

import (
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"k8s.io/klog/v2"
)

// An OOM kill of the writer is a marker gap like any other crash. The Go runtime
// does not size its heap to the container: without a soft limit it lets the heap
// double past the cgroup limit before collecting and the kernel kills the pod.
// SetMemoryLimitFromCgroup sets the runtime soft limit to memLimitFraction of the
// container memory limit, so the collector works harder near the limit instead.

const memLimitFraction = 0.9

// cgroup files that hold the container memory limit, v2 first.
var cgroupMemoryLimitFiles = []string{
	"/sys/fs/cgroup/memory.max",
	"/sys/fs/cgroup/memory/memory.limit_in_bytes",
}

// cgroupV1Unlimited is the smallest value cgroup v1 reports for "no limit"
// (the page-rounded max int64); anything at or above it means unlimited.
const cgroupV1Unlimited = int64(1) << 62

// memoryLimitFromCgroup returns the container memory limit in bytes, read with
// readFile from the first cgroup file that holds one.
func memoryLimitFromCgroup(readFile func(string) ([]byte, error)) (int64, bool) {
	for _, f := range cgroupMemoryLimitFiles {
		b, err := readFile(f)
		if err != nil {
			continue
		}
		v := strings.TrimSpace(string(b))
		if v == "" || v == "max" {
			return 0, false
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 || n >= cgroupV1Unlimited {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// softMemoryLimit is the runtime limit for a container limit of n bytes.
func softMemoryLimit(n int64) int64 {
	return int64(float64(n) * memLimitFraction)
}

// SetMemoryLimitFromCgroup applies the soft limit unless GOMEMLIMIT is set
// (an explicit setting always wins) or the container has no memory limit.
func SetMemoryLimitFromCgroup() {
	if os.Getenv("GOMEMLIMIT") != "" {
		return
	}
	n, ok := memoryLimitFromCgroup(os.ReadFile)
	if !ok {
		return
	}
	limit := softMemoryLimit(n)
	debug.SetMemoryLimit(limit)
	klog.InfoS("set the Go soft memory limit from the container limit", "containerLimitBytes", n, "softLimitBytes", limit)
}
