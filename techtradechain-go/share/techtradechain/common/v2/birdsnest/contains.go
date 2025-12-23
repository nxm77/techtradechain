/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package birdsnest contains
package birdsnest

// Contains returns whether the filter exists sync
func (b *BirdsNestImpl) Contains(key Key, rules ...RuleType) (bool, error) {
	if key == nil || key.Len() == 0 {
		return false, ErrKeyCannotBeEmpty
	}
	err := b.ValidateRule(key, rules...)
	if err != nil {
		return false, err
	}
	for i := range b.filters {
		contains, err := b.filters[i].Contains(key)
		if err != nil {
			return false, err
		}
		if contains {
			return true, nil
		}
	}
	// Does not exist in any filter
	return false, nil
}

// Contains returns whether the filter exists parallel
//func (b *BirdsNestImpl) Contains(key Key, rules ...common.RuleType) (bool, error) {
//	if key == nil || key.Len() == 0 {
//		return false, ErrKeyCannotBeEmpty
//	}
//	for _, rule := range rules {
//		r, ok := b.rules[rule]
//		if !ok {
//			continue
//		}
//		err := r.Validate(key)
//		if err != nil {
//			return false, err
//		}
//	}
//
//	var (
//		containsResult     = make(chan cts)
//		timeoutCtx, cancel = context.WithTimeout(context.Background(), time.Second*3)
//		runningTask        = len(b.filters)
//	)
//	defer cancel()
//	for i := range b.filters {
//		go b.containsTask(timeoutCtx, b.filters[i], key, containsResult)
//	}
//	for {
//		select {
//		case <-timeoutCtx.Done():
//			return false, errors.New("filter contains timeout")
//		case result := <-containsResult:
//			if result.IsError() {
//				return false, result.err
//			}
//			if result.contains {
//				return true, nil
//			}
//			runningTask--
//			if runningTask <= 0 {
//				return false, nil
//			}
//		}
//	}
//}
//
//func (b *BirdsNestImpl) containsTask(ctx context.Context, filter CuckooFilter, key Key, containsResult chan cts) {
//	select {
//	case <-ctx.Done():
//		return
//	default:
//		contains, err := filter.Contains(key)
//		containsResult <- cts{contains: contains, err: err}
//		return
//	}
//}
//
//type cts struct {
//	contains bool
//	err      error
//}
//
//func (c cts) IsError() bool {
//	return c.err != nil
//}
//
//func (c cts) Contains() bool {
//	return c.contains
//}
