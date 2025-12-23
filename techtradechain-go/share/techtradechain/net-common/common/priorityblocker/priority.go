/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package priorityblocker

import "sync"

// Priority for blocking with blocker.
type Priority uint8

const (
	// PriorityLevel0 level0
	PriorityLevel0 Priority = iota
	// PriorityLevel1 level1
	PriorityLevel1
	// PriorityLevel2 level2
	PriorityLevel2
	// PriorityLevel3 level3
	PriorityLevel3
	// PriorityLevel4 level4
	PriorityLevel4
	// PriorityLevel5 level5
	PriorityLevel5
	// PriorityLevel6 level6
	PriorityLevel6
	// PriorityLevel7 level7
	PriorityLevel7
	// PriorityLevel8 level8
	PriorityLevel8
	// PriorityLevel9 level9
	PriorityLevel9
)

// flagPriorityRecorder record the priority of flags.
type flagPriorityRecorder struct {
	m sync.Map
}

// SetPriority set the priority of the flag.
func (r *flagPriorityRecorder) SetPriority(flag string, p Priority) {
	r.m.LoadOrStore(flag, p)
}

// GetPriority query the priority of the flag.
// If it has not been set, default return PriorityLevel5.
func (r *flagPriorityRecorder) GetPriority(flag string) Priority {
	v, ok := r.m.Load(flag)
	if ok {
		return v.(Priority)
	}
	return PriorityLevel5
}
