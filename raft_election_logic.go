package raft

import (
	"math/rand"
	"time"
)

/*
startElectionTimer implements an election timer. It should be launched whenever
we want to start a timer towards becoming a candidate in a new election.
This function runs as a goroutine
*/

func (this *RaftNode) startElectionTimer() {
	timeoutDuration := time.Duration(3000+rand.Intn(3000)) * time.Millisecond
	this.mu.Lock()
	termStarted := this.currentTerm
	this.mu.Unlock()
	this.write_log("Election timer started: %v, with term=%d", timeoutDuration, termStarted)

	// Keep checking for a resolution
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		this.mu.Lock()

		// if node has become a leader
		if this.state != "Candidate" && this.state != "Follower" {
			this.mu.Unlock()
			return
		}

		// if node received requestVote or appendEntries of a higher term and updated itself
		if termStarted != this.currentTerm {
			this.mu.Unlock()
			return
		}

		// Start an election if we haven't heard from a leader or haven't voted for someone for the duration of the timeout.
		if elapsed := time.Since(this.lastElectionTimerStartedTime); elapsed >= timeoutDuration {
			this.startElection()
			this.mu.Unlock()
			return
		}
		this.mu.Unlock()
	}
}

// startElection starts a new election with this RN as a candidate.
func (this *RaftNode) startElection() {

	// Become a candidate
	this.state = "Candidate"
	// Increment the current term optimistically, hoping it's the latest
	this.currentTerm += 1
	// Aux info.
	termWhenVoteRequested := this.currentTerm
	this.lastElectionTimerStartedTime = time.Now()

	// Vote for Yourself! Very important.
	this.votedFor = this.id
	votesReceived := 1

	this.write_log("became Candidate with term=%d;", termWhenVoteRequested)

	// Send RequestVote RPCs to all other servers concurrently.
	for _, peerId := range this.peersIds {

		// Issue the RequestVoteRPCs in parallel
		go func(peerId int) {

			// This looks like a read lock? All other variables are goroutine-local, so not sure why there's a lock
			this.mu.Lock()
			var LastLogIndexWhenVoteRequested, LastLogTermWhenVoteRequested int

			if len(this.log) > 0 {
				// if there are already log entries, use that to find the "latest" term and index
				lastIndex := len(this.log) - 1
				LastLogIndexWhenVoteRequested, LastLogTermWhenVoteRequested = lastIndex, this.log[lastIndex].Term
			} else {
				// Otherwise just set to -1
				LastLogIndexWhenVoteRequested, LastLogTermWhenVoteRequested = -1, -1
			}
			this.mu.Unlock()

			// Create a RequestVoteRPC Request
			args := RequestVoteArgs{
				Term:         termWhenVoteRequested,
				CandidateId:  this.id,
				LastLogIndex: LastLogIndexWhenVoteRequested,
				LastLogTerm:  LastLogTermWhenVoteRequested,

				// MEGA sus. Perhaps a test infra thing, to simulate weak networks?
				Latency: rand.Intn(500), // Ignore Latency.
			}

			if LogVoteRequestMessages {
				this.write_log("sending RequestVote to %d: %+v", peerId, args)
			}

			var reply RequestVoteReply

			// Send the Actual RequestVote RPC Call
			if err := this.server.SendRPCCallTo(peerId, "RaftNode.RequestVote", args, &reply); err == nil {

				// using `defer` here is valid and does not cause lock contention because
				// despite being called in a loop, it is also called in an anon function that gets
				// passed to a goroutine, and not in a loop isolated.
				// It is NOT being assumed that the Defer will execute at the ending of the block, but
				// at the end of the function.
				// GG, Murali :D
				this.mu.Lock()
				defer this.mu.Unlock()

				if LogVoteRequestMessages {
					// Util check to see if we want to log or not
					this.write_log("received RequestVoteReply from %d: %+v", peerId, reply)
				}
				// Looks like we became leader midway
				if this.state != "Candidate" {
					this.write_log("State changed from Candidate to %s", this.state)
					return
				}

				// IMPLEMENT HANDLING THE VOTEREQUEST's REPLY;
				// You probably need to have implemented becomeFollower before this.
				// Check whether or not we have quorum - if we do, we are leader, and make those changes
				// Else become follower
				if reply.Term > this.currentTerm {

					// DONE:TODO
					// Become Follower
					this.becomeFollower(args.Term)

				} else if reply.Term == this.currentTerm {

					// DONE:TODO
					// Considered a vote for this term?
					if reply.VoteGranted == true {
						votesReceived++

						// If we have quorum, become leader
						if votesReceived > (len(this.peersIds) / 2) {
							this.startLeader()
						}

					}
				}
			}
		}(peerId)
	}

	// Run another election timer, in case this election is not successful.
	go this.startElectionTimer()
}

// becomeFollower sets a node to be a follower and resets its state.
func (this *RaftNode) becomeFollower(term int) {
	this.write_log("became Follower with term=%d; log=%v", term, this.log)

	this.mu.Lock()
	defer this.mu.Unlock()

	// Set the state to be follower
	this.state = "Follower"
	// Set the term to be the term supplied
	this.currentTerm = term

	// HACK @Author = Adarsh Liju Abraham

	// I feel a reset is required here
	this.votedFor = -1
	this.lastElectionTimerStartedTime = time.Now()
	// HACK unsure about this, but let's see.
	// I feel this will work for now
	go this.startElectionTimer()
}
