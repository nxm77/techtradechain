/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package priorityblocker

import (
	"sync"
)

// Blocker provides a function which can block some process.
// Unblock strategy bases on Priority.
// The higher the priority, the earlier it will be unblocked.
// Unblocking is done by consuming tickets.
// If ticket chan is nil when new a Blocker,
// Blocker will keep printing tickets itself.
type Blocker struct {
	once sync.Once
	// times of consecutive unblocking with the same priority level allowed,
	// if reach this value, a few lower priority unblocking will be run.
	consecutiveCountAllowed uint8
	// times of consecutive unblocking with the lower priority level
	// after times of consecutive unblocking with the same priority level reaching the value allowed.
	printLowLevelTicketsTimes int

	rec                     *flagPriorityRecorder // flag priority recorder
	priorityChannel         [10]chan struct{}     // tickets chan for each priorities
	levelConsecutiveCount   [10]uint8             // times of consecutive consuming tickets of each priorities
	currentConsecutiveLevel Priority              // priority of last unblock
	outerTicketC            <-chan struct{}       // tickets chan
	closeC                  chan struct{}
}

// NewBlocker create a new Blocker instance.
func NewBlocker(ticketChan <-chan struct{}) *Blocker {
	b := &Blocker{
		rec:                       &flagPriorityRecorder{m: sync.Map{}},
		consecutiveCountAllowed:   8,
		printLowLevelTicketsTimes: 2,
		priorityChannel:           [10]chan struct{}{},
		levelConsecutiveCount:     [10]uint8{},
		currentConsecutiveLevel:   PriorityLevel9,
		outerTicketC:              ticketChan,
		closeC:                    make(chan struct{}),
	}
	for i := 0; i < len(b.priorityChannel); i++ {
		b.priorityChannel[i] = make(chan struct{})
	}
	return b
}

func (b *Blocker) printTickets() {
	for {
		select {
		case <-b.closeC:
			for i := range b.priorityChannel {
				close(b.priorityChannel[i])
			}
			return
		default:
			b.printTicket(PriorityLevel9)
		}
	}
}

func (b *Blocker) printTicket(p Priority) {
	var ticket struct{}
	if b.outerTicketC != nil {
		// if print tickets with outer printer, wait for ticket
		ticket = <-b.outerTicketC
	} else {
		// if print tickets by self, always print new ticket
		ticket = struct{}{}
	}
	select {
	case <-b.closeC:
		return
	case b.priorityChannel[p] <- ticket:
		// if some processes blocking on this priority, consume ticket.
		// then check consecutive consuming
		b.checkConsecutiveCount(p)
		return
	default:
		if p > PriorityLevel0 {
			// if no process blocking on this priority, try next lower priority until PriorityLevel0
			b.printTicket(p - 1)
			return
		}
		// if no process blocking on each priorities, waiting for next process blocking on any priority
		select {
		case <-b.closeC:
			return
		case b.priorityChannel[PriorityLevel9] <- ticket:
			b.checkConsecutiveCount(PriorityLevel9)
		case b.priorityChannel[PriorityLevel8] <- ticket:
			b.checkConsecutiveCount(PriorityLevel8)
		case b.priorityChannel[PriorityLevel7] <- ticket:
			b.checkConsecutiveCount(PriorityLevel7)
		case b.priorityChannel[PriorityLevel6] <- ticket:
			b.checkConsecutiveCount(PriorityLevel6)
		case b.priorityChannel[PriorityLevel5] <- ticket:
			b.checkConsecutiveCount(PriorityLevel5)
		case b.priorityChannel[PriorityLevel4] <- ticket:
			b.checkConsecutiveCount(PriorityLevel4)
		case b.priorityChannel[PriorityLevel3] <- ticket:
			b.checkConsecutiveCount(PriorityLevel3)
		case b.priorityChannel[PriorityLevel2] <- ticket:
			b.checkConsecutiveCount(PriorityLevel2)
		case b.priorityChannel[PriorityLevel1] <- ticket:
			b.checkConsecutiveCount(PriorityLevel1)
		case b.priorityChannel[PriorityLevel0] <- ticket:
			b.checkConsecutiveCount(PriorityLevel0)
		}
	}
}

func (b *Blocker) checkConsecutiveCount(p Priority) {
	if b.currentConsecutiveLevel == p {
		// if consecutive, count++
		b.levelConsecutiveCount[p]++
		if b.levelConsecutiveCount[p]%b.consecutiveCountAllowed == 0 {
			// if reach the value allowed, try to consume a few tickets for lower priority blocking.
			b.printLowLevelTickets(p, b.printLowLevelTicketsTimes)
		}
	} else {
		// if not consecutive, reset count=1
		b.currentConsecutiveLevel = p
		b.levelConsecutiveCount[p] = 1
	}
}

func (b *Blocker) checkConsecutiveCountLow(low Priority) {
	b.levelConsecutiveCount[low]++
	if b.levelConsecutiveCount[low]%b.consecutiveCountAllowed == 0 {
		b.printLowLevelTickets(low, b.printLowLevelTicketsTimes)
	}
}

func (b *Blocker) printLowLevelTickets(p Priority, times int) {
	if p == PriorityLevel0 {
		return
	}
	var ticket struct{}
	if b.outerTicketC != nil {
		ticket = <-b.outerTicketC
	} else {
		ticket = struct{}{}
	}
outer:
	for i := 0; i < times; i++ {
		for j := p; j > PriorityLevel0; j-- {
			low := j - 1
			select {
			case b.priorityChannel[low] <- ticket:
				b.checkConsecutiveCountLow(low)
				continue outer
			default:

			}
		}
		break
	}
}

// Run start a goroutine to print tickets.
func (b *Blocker) Run() {
	b.once.Do(func() {
		go b.printTickets()
	})
}

// Close the blocker.
func (b *Blocker) Close() {
	close(b.closeC)
}

// Block the process, until a ticket with the priority of the flag consumed.
func (b *Blocker) Block(flag string) {
	p := b.rec.GetPriority(flag)
	<-b.priorityChannel[p]
}

// SetPriority set the priority of the flag.
func (b *Blocker) SetPriority(flag string, p Priority) {
	b.rec.SetPriority(flag, p)
}

// GetPriority query the priority of the flag.
// If never set, will return PriorityLevel5.
func (b *Blocker) GetPriority(flag string) Priority {
	return b.rec.GetPriority(flag)
}
