package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltdb/bolt"
	"github.com/darshanmaiya/raft/config"
	"github.com/darshanmaiya/raft/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

type NodeState uint8

const (
	Follower NodeState = iota << 1
	Candidate
	Leader
)

// Raft represents an instance of raft implementing the gRPC
// server interface.
type Raft struct {
	ServerID uint32
	Port     string

	logStore  *LogStore
	metaStore *MetaStore

	// Volatile state on all servers:
	//
	// CommitIndex is the index of highest log entry known to be committed
	// (initialized to 0, increases monotonically)
	CommitIndex uint32

	// LastApplied is the index of highest log entry applied to state machine
	// (initialized to 0, increases monotonically)
	LastApplied uint32

	// Volatile state on leaders:
	// (Reinitialized after election)
	//
	// NextIndex marks for each server, index of the next log entry to send
	// to that server (initialized to leade	last log index + 1)
	NextIndex map[int]uint32

	// MatchIndex marks for each server, index of highest log entry known
	// to be replicated on server (initialized to 0, increases monotonically)
	MatchIndex map[int]uint32

	// Participants is the set of all participating servers in Raft.
	Participants map[int]string
	// Reverse mapping of server IPs to IDs
	ServersToIdMap map[string]int
	// Next Participant ID
	NextParticipantId int

	// Leader ID from whom the last message was received
	LeaderId int

	stateTransition chan NodeState
	stateUpdate     chan struct{}

	electionLost chan struct{}
	msgReceived  chan struct{}

	nodes map[uint32]*rpcConn

	configInFlight bool
	pendingConfig  map[uint32]string

	wg   sync.WaitGroup
	quit chan struct{}
}

type rpcConn struct {
	isSyncing uint32 // only to be used atomically

	conn   *grpc.ClientConn
	client raft.RaftClient
}

// newRaft creates a new instance of raft given its server id.
func newRaft(serverID uint32) (*Raft, error) {
	db, err := bolt.Open(fmt.Sprintf("log-%d.db", serverID), 0600, nil)
	if err != nil {
		return nil, err
	}

	metaStore, err := NewMetaStore(db)
	if err != nil {
		return nil, err
	}

	logStore, err := NewLogStore(db)
	if err != nil {
		return nil, err
	}

	s := &Raft{
		ServerID:       serverID,
		logStore:       logStore,
		metaStore:      metaStore,
		Participants:   make(map[int]string),
		ServersToIdMap: make(map[string]int),

		NextIndex:  make(map[int]uint32),
		MatchIndex: make(map[int]uint32),

		LeaderId: -1,

		stateTransition: make(chan NodeState, 1),
		stateUpdate:     make(chan struct{}),

		electionLost: make(chan struct{}, 1),
		msgReceived:  make(chan struct{}, 1),

		nodes:         make(map[uint32]*rpcConn),
		pendingConfig: make(map[uint32]string),

		quit: make(chan struct{}, 1),
	}

	// Get the map of participants from the global config.
	s.Participants, err = config.GetServersFromConfig()
	if err != nil {
		return nil, err
	}
	s.NextParticipantId = len(s.Participants)
	for i, value := range s.Participants {
		s.ServersToIdMap[value] = i
	}

	// Set the port for this server
	if _, ok := s.Participants[int(serverID)]; !ok {
		s.Port = strconv.Itoa(50000 + int(serverID))
	} else {
		s.Port = strings.Split(s.Participants[int(serverID)], ":")[1]
	}

	for i, server := range s.Participants {
		if i == int(s.ServerID) {
			continue
		}

		var opts []grpc.DialOption
		opts = append(opts, grpc.WithInsecure())
		opts = append(opts, grpc.WithBackoffConfig(grpc.BackoffConfig{MaxDelay: 100 * time.Millisecond}))
		opts = append(opts, grpc.WithTimeout(time.Minute*5))
		conn, err := grpc.Dial(server, opts...)
		if err != nil {
			return nil, err
		}

		client := raft.NewRaftClient(conn)
		s.nodes[uint32(i)] = &rpcConn{
			conn:   conn,
			client: client,
		}
	}

	fmt.Println("Participant servers initialized successfully.")
	s.printParticipants()
	return s, nil
}

func (r *Raft) Start() error {
	r.wg.Add(1)
	go r.coordinator()

	return nil
}

func (s *Raft) Post(ctx context.Context, req *raft.PostArgs) (*raft.PostResponse, error) {
	state, err := s.metaStore.FetchState()
	if err != nil {
		return &raft.PostResponse{}, err
	}

	if state == Candidate {
		// State is candidate, wait for a signal before responding
		<-s.stateUpdate
	}

	if state == Follower {
		log.Println("redirectiong client to leader: ", s.LeaderId)
		return &raft.PostResponse{
			Response: &raft.RPCResponse{
				Success:  false,
				Message:  "I'm not the leader",
				LeaderId: uint32(s.LeaderId),
			},
		}, nil
	} else if state == Leader {
		// Replicate the new entry to all peers synchronously before we
		// reply to the client.
		ok, err := s.replicateEntry(req, nil)
		if err != nil {
			log.Println("error replicating entry to follower")
			return &raft.PostResponse{}, err
		} else if !ok {
			log.Println("unable to obtain majority to replicate entries")
			return &raft.PostResponse{}, fmt.Errorf("unable to get majority")
		}

		// AppendEntries here
		return &raft.PostResponse{
			Response: &raft.RPCResponse{
				Success:  true,
				Message:  "Message posted",
				LeaderId: uint32(s.ServerID),
			},
		}, nil
	}

	return &raft.PostResponse{}, nil
}

func (s *Raft) Lookup(ctx context.Context, req *raft.LookupArgs) (*raft.LookupResponse, error) {

	entries, err := s.logStore.FetchAllEntries()
	if err != nil {
		return nil, err
	}

	state, err := s.metaStore.FetchState()
	if err != nil {
		return &raft.LookupResponse{}, err
	}

	if state == Candidate {
		// State is candidate, wait for a signal before responding
		<-s.stateUpdate
	}

	if state == Follower {
		log.Println("redirectiong client to leader: ", s.LeaderId)
		return &raft.LookupResponse{
			Response: &raft.RPCResponse{
				Success:  false,
				Message:  "I'm not the leader",
				LeaderId: uint32(s.LeaderId),
			},
		}, nil
	} else if state == Leader {
		return &raft.LookupResponse{
			Response: &raft.RPCResponse{
				Success:  true,
				Message:  "Lookup successful",
				LeaderId: uint32(s.LeaderId),
			},
			Entries: entries,
		}, nil
	}

	return &raft.LookupResponse{}, nil
}

func (s *Raft) Config(ctx context.Context, req *raft.ConfigArgs) (*raft.ConfigResponse, error) {

	state, err := s.metaStore.FetchState()
	if err != nil {
		return &raft.ConfigResponse{}, err
	}

	if state == Candidate {
		// State is candidate, wait for a signal before responding
		<-s.stateUpdate
	}

	if state == Follower {
		log.Println("redirectiong client to leader: ", s.LeaderId)
		return &raft.ConfigResponse{
			Response: &raft.RPCResponse{
				Success:  false,
				Message:  "I'm not the leader",
				LeaderId: uint32(s.LeaderId),
			},
		}, nil
	} else if state == Leader {

		// Change the config
		newServers := make(map[int]string)
		newServersToIdMap := make(map[string]int)
		newConfigChange := make(map[uint32]string)

		newParticipants := make(map[int]string)

		serversList := req.Servers

		for _, val := range serversList {
			// If it exists retain its ID
			if id, ok := s.ServersToIdMap[val]; ok {
				newServers[id] = val
				newConfigChange[uint32(id)] = val
				newServersToIdMap[val] = id
			} else {
				newServers[s.NextParticipantId] = val
				newConfigChange[uint32(s.NextParticipantId)] = val
				newServersToIdMap[val] = s.NextParticipantId

				newParticipants[s.NextParticipantId] = val

				s.NextParticipantId++
			}
		}

		oldServersConfig := make(map[uint32]string)
		for id, val := range s.Participants {
			oldServersConfig[uint32(id)] = val
		}

		// Replicate the new entry to all peers synchronously before we
		// reply to the client.
		// Replicate old + new
		ok, err := s.replicateEntry(nil, &raft.ConfigChange{
			Complete:   false,
			NewServers: newConfigChange,
			OldServers: oldServersConfig,
		})

		if err != nil {
			log.Println("error replicating entry to follower")
			return &raft.ConfigResponse{}, err
		} else if !ok {
			log.Println("unable to obtain majority to replicate entries")
			return &raft.ConfigResponse{}, fmt.Errorf("unable to get majority")
		}

		// get current log length
		logLength, err := s.logStore.LogLength()
		if err != nil {
			log.Println("error getting log length")
			return &raft.ConfigResponse{}, err
		}

		// establish new connections
		for i, server := range newParticipants {
			var opts []grpc.DialOption
			opts = append(opts, grpc.WithInsecure())
			opts = append(opts, grpc.WithBackoffConfig(grpc.BackoffConfig{MaxDelay: 100 * time.Millisecond}))
			opts = append(opts, grpc.WithTimeout(time.Minute*5))
			conn, err := grpc.Dial(server, opts...)
			if err != nil {
				return nil, err
			}

			client := raft.NewRaftClient(conn)
			s.nodes[uint32(i)] = &rpcConn{
				conn:   conn,
				client: client,
			}
			s.NextIndex[i] = logLength
		}

		// Replicate new
		ok, err = s.replicateEntry(nil, &raft.ConfigChange{
			Complete:   true,
			NewServers: newConfigChange,
		})

		if err != nil {
			log.Println("error replicating entry to follower")
			return &raft.ConfigResponse{}, err
		} else if !ok {
			log.Println("unable to obtain majority to replicate entries")
			return &raft.ConfigResponse{}, fmt.Errorf("unable to get majority")
		}

		// Remove old participants
		for id, _ := range s.Participants {
			if _, ok := newServers[id]; !ok {
				s.nodes[uint32(id)].conn.Close()
				delete(s.nodes, uint32(id))
			}
		}

		s.Participants = newServers
		s.ServersToIdMap = newServersToIdMap

		newConfig := make(map[uint32]string)
		for servId, servIp := range s.Participants {
			newConfig[uint32(servId)] = servIp
		}

		return &raft.ConfigResponse{
			Response: &raft.RPCResponse{
				Success:  true,
				Message:  "Config change request accepted successfully",
				LeaderId: uint32(s.ServerID),
			},
			Servers: newConfig,
		}, nil
	}

	return &raft.ConfigResponse{}, nil
}

func (s *Raft) RequestVote(ctx context.Context, req *raft.RequestVoteArgs) (*raft.RequestVoteResponse, error) {
	var granted bool

	// If we've already voted for a new candidate during this term, then
	// decline the request, otherwise accept this as a new candidate and
	// record our vote.
	currentTerm, err := s.metaStore.FetchCurrentTerm()
	if err != nil {
		return nil, err
	}
	state, err := s.metaStore.FetchState()
	if err != nil {
		return nil, err
	}
	votedFor, err := s.metaStore.FetchVotedFor()
	if err != nil {
		return nil, err
	}
	logLength, err := s.logStore.LogLength()
	if err != nil {
		return nil, err
	}

	if req.Term < uint32(currentTerm) || votedFor != -1 || state != Follower {
		log.Println("candidate's term is outdated, ignoring vote")
		granted = false
	} else {
		entry, err := s.logStore.FetchEntry(req.LastLogIndex)
		if err != nil {
			return nil, err
		}

		// TODO(roasbeef): safety not enfored?
		if req.LastLogIndex < logLength {
			log.Printf("candidate's log is out of date, denying: %v vs %v\n",
				req.LastLogIndex, logLength)
			granted = false
		} else if entry != nil && entry.Term != req.LastLogTerm {
			log.Printf("candidate's log term is out of date, denying: %v vs %v\n",
				req.LastLogTerm, entry.Term)
			granted = false
		} else {

			if err := s.metaStore.UpdateCurrentTerm(req.Term); err != nil {
				return nil, err
			}
			currentTerm = req.Term

			if err := s.metaStore.UpdateVotedFor(int32(req.CandidateId)); err != nil {
				return nil, err
			}

			granted = true

			s.msgReceived <- struct{}{}
		}
	}

	fmt.Printf("Server %d is requesting vote..\n", req.CandidateId)
	return &raft.RequestVoteResponse{
		Term:        uint32(currentTerm),
		VoteGranted: granted,
	}, nil
}

func (s *Raft) AppendEntries(ctx context.Context, req *raft.AppendEntriesArgs) (*raft.AppendEntriesResponse, error) {
	state, err := s.metaStore.FetchState()
	if err != nil {
		return nil, err
	}

	// If we get a heart beat with an empty log and we're a candidate, then
	// we lost the election.
	if state == Candidate && len(req.Entries) == 0 {
		log.Printf("I lost the election because I received an empty "+
			"AppendEntries from Server %d\n", req.LeaderId)
		s.LeaderId = int(req.LeaderId)
		if err := s.metaStore.UpdateCurrentTerm(req.Term); err != nil {
			return &raft.AppendEntriesResponse{req.Term, false}, nil
		}
		s.electionLost <- struct{}{}
	}

	// If we're not the leader, then reset the server down timer, and also
	// record the current leader.
	if state != Leader {
		s.LeaderId = int(req.LeaderId)
		s.msgReceived <- struct{}{}
	}

	currentTerm, err := s.metaStore.FetchCurrentTerm()
	if err != nil {
		return &raft.AppendEntriesResponse{currentTerm, false}, err
	}

	// With the initial checks complete, attempt to add this new entry to
	// our log, responding appropritely.
	success := true
	if req.Term != currentTerm {
		log.Printf("current term %d is behind leader's term %d, updating\n",
			currentTerm, req.Term)
		if err := s.metaStore.UpdateCurrentTerm(req.Term); err != nil {
			return &raft.AppendEntriesResponse{currentTerm, success}, nil
		}
	}

	prevLogEntry, err := s.logStore.FetchEntry(req.PrevLogIndex)
	if err != nil {
		return &raft.AppendEntriesResponse{currentTerm, false}, err
	}

	// If this previous log entry doesn't match the leader's advertised
	// term, then there's an inconsistency, so return false.
	if req.PrevLogTerm != 0 && (prevLogEntry == nil || prevLogEntry.Term != req.PrevLogTerm) {
		success = false
		if prevLogEntry != nil {
			log.Printf("prev entry has term %v, leader advertised %d \n",
				prevLogEntry.Term, req.PrevLogTerm)
		} else {
			log.Printf("Entry not found!")
		}

		return &raft.AppendEntriesResponse{currentTerm, success}, nil
	}

	// Check to see if we already have an entry for this index, if so then
	// delete it and append this one instead.
	if logEntry, err := s.logStore.FetchEntry(req.LeaderCommit); err != nil {
		return &raft.AppendEntriesResponse{currentTerm, false}, err
	} else if logEntry != nil && logEntry.Term != currentTerm && len(req.Entries) > 0 {
		log.Printf("already have entry at index %d, deleting\n", req.LeaderCommit)
		if err := s.logStore.RemoveEntry(req.LeaderCommit); err != nil {
			return &raft.AppendEntriesResponse{currentTerm, false}, err
		}
	}

	if len(req.Entries) != 0 {
		log.Println("add new entry at index: ", req.LeaderCommit)
		if err := s.logStore.AddEntry(req.LeaderCommit, req.Entries[0]); err != nil {
			return &raft.AppendEntriesResponse{currentTerm, false}, err
		}
	}

	// A config change request
	if len(req.Entries) > 0 && req.Entries[0].ConfigChange != nil {
		newConfig := req.Entries[0].ConfigChange.NewServers

		if !req.Entries[0].ConfigChange.Complete {
			s.configInFlight = false
			s.pendingConfig = nil

			// Establish new connections
			for id, newServerIp := range newConfig {
				// if id does not exists in s.nodes
				// add to s.nodes
				if _, ok := s.nodes[uint32(id)]; !ok {
					var opts []grpc.DialOption
					opts = append(opts, grpc.WithInsecure())
					opts = append(opts, grpc.WithBackoffConfig(grpc.BackoffConfig{MaxDelay: 100 * time.Millisecond}))
					opts = append(opts, grpc.WithTimeout(time.Minute*5))
					conn, err := grpc.Dial(newServerIp, opts...)
					if err != nil {
						return nil, err
					}

					client := raft.NewRaftClient(conn)
					s.nodes[uint32(id)] = &rpcConn{
						conn:   conn,
						client: client,
					}
				}
			}
		} else {
			s.configInFlight = true
			s.pendingConfig = newConfig

			// Remove old connections
			for id, _ := range s.Participants {
				if _, ok := newConfig[uint32(id)]; !ok {
					s.nodes[uint32(id)].conn.Close()
					delete(s.nodes, uint32(id))
				}
			}

			// Reinitialize new participants
			s.Participants = make(map[int]string)
			s.ServersToIdMap = make(map[string]int)
			var maxId int
			for id, val := range newConfig {
				if int(id) > maxId {
					maxId = int(id)
				}
				s.Participants[int(id)] = val
				s.ServersToIdMap[val] = int(id)
			}
			s.NextParticipantId = maxId
		}
	}

	return &raft.AppendEntriesResponse{currentTerm, success}, nil
}

func (s *Raft) printParticipants() {
	fmt.Println("Available servers are:\n")

	for i, value := range s.Participants {
		if i == int(s.ServerID) {
			fmt.Printf(">> Server %d @ %v <<\n", i, value)
		} else {
			fmt.Printf("Server %d @ %s\n", i, value)
		}
	}
}

var _ raft.RaftServer = (*Raft)(nil)

var (
	port     = flag.Int("port", 50000, "The server port")
	serverID = flag.Int("id", 0, "The server ID")
)

func (r *Raft) coordinator() {
	serverDownTimeout := 150 * time.Millisecond
	heartBeatTimeout := 100 * time.Millisecond

	// TODO(roasbeef): possible goroutine leak by just
	// using time.After?
	serverDownTimer := time.After(serverDownTimeout)
	var electionTimer <-chan time.Time
	var heartBeatTimer <-chan time.Time

	var electionCancel chan struct{}
out:
	for {
		select {
		case <-serverDownTimer:
			// Haven't received message from leader in serverDownTimeout
			// period. So start an election after a random period of time.
			electionBackOff := time.Duration(rand.Intn(140)+10) * time.Millisecond

			log.Printf("Haven't heard from the leader, backing off to election: %v\n",
				electionBackOff)
			r.LeaderId = -1
			electionTimer = time.After(electionBackOff)
		case <-electionTimer:
			log.Println("Election back off triggered, requesting votes")
			if err := r.metaStore.UpdateState(Candidate); err != nil {
				log.Println("unable to update state")
			}

			electionTimeout := time.After(serverDownTimeout)
			serverDownTimer = nil

			if err := r.metaStore.IncrementTerm(); err != nil {
				log.Println("unable to increment term")
			}
			if err := r.metaStore.UpdateVotedFor(-1); err != nil {
				log.Println("unable to update voted for")
			}

			electionCancel = make(chan struct{}, 1)
			r.stateUpdate = make(chan struct{})
			go r.startElection(electionCancel, electionTimeout, serverDownTimeout)
		case <-heartBeatTimer:
			// Send out heart beats to all participants, and reset the
			// timer.
			log.Println("Sending heart beat")
			go r.sendHeatBeat()

			heartBeatTimer = time.After(heartBeatTimeout)
		case <-r.msgReceived:
			log.Println("Received heartbeat, leader is still up")
			// We got a message from the leader before the timeout was
			// up. So reset it.
			serverDownTimer = time.After(serverDownTimeout)
			electionTimer = nil
		case <-r.electionLost:
			if electionCancel != nil {
				electionCancel <- struct{}{}
			}
			log.Println("I lost the election, switching to follower")

			if err := r.metaStore.UpdateState(Follower); err != nil {
				log.Println("unable to update state")
			}

			// The election was lost, cancel the election timer.
			electionTimer = nil
			heartBeatTimer = nil
			electionCancel = nil
			serverDownTimer = time.After(serverDownTimeout)
		case newState := <-r.stateTransition:
			if err := r.metaStore.UpdateState(newState); err != nil {
				log.Println("unable to update state")
			}

			if newState == Leader {
				log.Println("I won election, sending heartbeat")

				// Reset all the timers, and trigger the heart beat timer.
				electionTimer = nil
				serverDownTimer = nil

				go r.sendHeatBeat()

				heartBeatTimer = time.After(heartBeatTimeout)

				// Reset the match index, and update the next
				// index for all followers.
				logLength, err := r.logStore.LogLength()
				if err != nil {
					log.Println("unable to get logindex")
				}
				atomic.StoreUint32(&r.CommitIndex, logLength)
				r.MatchIndex = make(map[int]uint32)
				for nodeId, _ := range r.nodes {
					r.NextIndex[int(nodeId)] = logLength + 1
				}
			} else {
				// This is triggered when there was a stalemate in the election
				log.Println("no winner of election, going back to follower")
				if err := r.metaStore.UpdateVotedFor(-1); err != nil {
					log.Println("unable to update voted for")
				}
				heartBeatTimer = nil
				serverDownTimer = time.After(serverDownTimeout)
			}
			close(r.stateUpdate)
			r.stateUpdate = make(chan struct{})
		case <-r.quit:
			break out
		}
	}

	r.wg.Done()
}

func (s *Raft) sendHeatBeat() {
	term, err := s.metaStore.FetchCurrentTerm()
	if err != nil {
		log.Println("unable to fetch term")
		return
	}
	logLength, err := s.logStore.LogLength()
	if err != nil {
		log.Println("unable to get log length")
	}
	lastEntry, err := s.logStore.FetchEntry(logLength - 1)
	if err != nil {
		log.Println("unable to fetch entry")
	}

	var lastTerm uint32
	if lastEntry == nil {
		lastTerm = 0
	} else {
		lastTerm = lastEntry.Term
	}

	for nodeId, node := range s.nodes {
		go func(nodeId uint32, node *rpcConn) {
			// If the connection isn't in the ready state, then it isn't
			// eligible for voting.
			connState, err := node.conn.State()
			if err != nil {
				return
			}
			if connState != grpc.Ready {
				s.NextIndex[int(nodeId)] = logLength
				return
			}

			if atomic.LoadUint32(&node.isSyncing) == 1 {
				log.Printf("node %v is syncing, skipping heartbeat\n",
					nodeId)
				return
			}

			args := &raft.AppendEntriesArgs{
				Term:         uint32(term),
				LeaderId:     s.ServerID,
				PrevLogIndex: logLength - 1,
				PrevLogTerm:  lastTerm,
				LeaderCommit: logLength,
			}

			resp, err := node.client.AppendEntries(context.Background(), args)
			if err != nil {
				log.Println("Server during heartbeat error:", err)
				return
			}

			if !resp.Success && resp.Term == term {
				log.Printf("node %v is behind, syncing up\n", nodeId)

				// Mark the node as currently syncing so future
				// heartbeats don't duplicate the process.
				atomic.StoreUint32(&node.isSyncing, 1)
				if _, err := s.syncFollower(logLength, term, nodeId, node); err != nil {
					log.Println("unable to sync follower: ", err)
				}
				atomic.StoreUint32(&node.isSyncing, 0)
			}
		}(nodeId, node)
	}
}

func (s *Raft) startElection(cancelSignal chan struct{},
	electionTimeout <-chan time.Time, timeoutDuration time.Duration) {

	fmt.Println("Sending RequestVoteRPC...")

	majority := (len(s.Participants) / 2) + 1
	response := make(chan raft.RequestVoteResponse, len(s.Participants))
	successVotes := 1

	term, err := s.metaStore.FetchCurrentTerm()
	if err != nil {
		log.Println("unable to get current term")
	}
	logLength, err := s.logStore.LogLength()
	if err != nil {
		log.Println("unable to get log length")
	}
	lastEntry, err := s.logStore.FetchEntry(logLength - 1)
	if err != nil {
		log.Println("unable to fetch entry")
	}

	var lastTerm uint32
	if lastEntry == nil {
		lastTerm = 0
	} else {
		lastTerm = lastEntry.Term
	}

	for _, node := range s.nodes {
		// If the connection isn't in the ready state, then it isn't
		// eligible for voting.
		connState, err := node.conn.State()
		if err != nil {
			continue
		}
		if connState != grpc.Ready {
			continue

		}

		go func(n raft.RaftClient) {
			args := &raft.RequestVoteArgs{
				Term:         term,
				CandidateId:  s.ServerID,
				LastLogIndex: logLength,
			}
			args.LastLogTerm = lastTerm
			fmt.Println("Sending request vote...")
			reply, err := n.RequestVote(context.Background(), args)
			fmt.Println("Received request vote...")
			if err != nil {
				log.Print("Server error:", err)
				response <- raft.RequestVoteResponse{
					VoteGranted: false,
				}
			} else {
				response <- raft.RequestVoteResponse{
					Term:        reply.Term,
					VoteGranted: reply.VoteGranted,
				}
			}
		}(node.client)
	}

	for i := 0; i < len(s.Participants); i++ {
		select {
		case <-cancelSignal:
			return
		case <-electionTimeout:
			s.stateTransition <- Follower
			return
		case reply := <-response:
			if reply.VoteGranted {
				successVotes++
			}

			if successVotes >= majority {
				s.stateTransition <- Leader
				return
			}
		}
	}
}

func (r *Raft) replicateEntry(newPost *raft.PostArgs, newConfig *raft.ConfigChange) (bool, error) {
	logLength, err := r.logStore.LogLength()
	if err != nil {
		return false, err
	}

	currentTerm, err := r.metaStore.FetchCurrentTerm()
	if err != nil {
		return false, err
	}

	// First write the new entry into our log.
	entry := &raft.LogEntry{}
	if newPost != nil {
		entry = &raft.LogEntry{
			Term: currentTerm,
			Msg:  newPost.Msg,
		}
	} else {
		entry = &raft.LogEntry{
			Term:         currentTerm,
			ConfigChange: newConfig,
		}
	}

	logLength += 1
	log.Printf("writing new entry at index %d \n", logLength)
	log.Println("current term: ", currentTerm)
	if err := r.logStore.AddEntry(logLength, entry); err != nil {
		return false, err
	}

	// Once we've written out to our local log, replicate the entry to all
	// followers, doing any re-orgs required to bring them up to date.
	numSuccess := 1
	numNew := 0
	numOld := 0
	for nodeId, node := range r.nodes {
		// If the connection isn't in the ready state, then it isn't
		// eligible for voting.
		connState, err := node.conn.State()
		if err != nil {
			continue
		}
		if connState != grpc.Ready {
			r.NextIndex[int(nodeId)] = logLength
			continue
		}

		atomic.StoreUint32(&node.isSyncing, 1)
		success, err := r.syncFollower(logLength, currentTerm, nodeId, node)
		if err != nil {
			log.Println("unable to replicate log")
			atomic.StoreUint32(&node.isSyncing, 0)
			continue
		}
		atomic.StoreUint32(&node.isSyncing, 0)

		if success {
			numSuccess += 1
		}

		if r.configInFlight {
			if _, ok := r.pendingConfig[nodeId]; ok {
				numNew += 1
			} else {
				numOld += 1
			}
		}
	}

	// If we're in the middle of a configuration change, then we need to
	// ensure we have a majority from both the new and old config.
	if !r.configInFlight {
		majority := (len(r.Participants) / 2) + 1
		if numSuccess >= majority {
			atomic.AddUint32(&r.CommitIndex, 1)
			log.Println("majority obtained, commit index: ", r.CommitIndex)
			return true, nil
		} else {
			log.Println("unable to obtain majority")
			return false, nil
		}
	} else {
		newMajority := (len(r.pendingConfig) / 2) + 1
		oldMajority := (len(r.Participants) / 2) + 1
		if numNew >= newMajority && numOld >= oldMajority {
			atomic.AddUint32(&r.CommitIndex, 1)
			log.Println("majority obtained, commit index: ", r.CommitIndex)
			return true, nil
		} else {
			log.Println("unable to obtain majority")
			return false, nil
		}
	}
}

func (r *Raft) syncFollower(logLength, currentTerm, nodeId uint32, node *rpcConn) (bool, error) {
	// Attempt to replicate out this new entry to this follower. In
	// the case the consistency check fails on the follower side,
	// we decrement the index we're attempting to replicate and
	// repeat the process.
	caughtUp := false
	for !caughtUp {
		// Grab the nextIndex for this peer, which is the entry we'll
		// replicate out.
		nextIndex := r.NextIndex[int(nodeId)]
		if nextIndex == 0 {
			nextIndex = 1
			r.NextIndex[int(nodeId)] = 1
		}
		logEntry, err := r.logStore.FetchEntry(nextIndex)
		if err != nil {
			return false, err
		}

		prevLogEntry, err := r.logStore.FetchEntry(nextIndex - 1)
		if err != nil {
			return false, err
		}

		var lastTerm uint32
		if prevLogEntry == nil {
			lastTerm = 0
		} else {
			lastTerm = prevLogEntry.Term
		}

		log.Printf("replicating entry %d to node %d\n",
			nextIndex, nodeId)

		args := &raft.AppendEntriesArgs{
			Term:         uint32(currentTerm),
			LeaderId:     r.ServerID,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  lastTerm,
			Entries: []*raft.LogEntry{
				logEntry,
			},
			LeaderCommit: nextIndex,
		}
		resp, err := node.client.AppendEntries(context.Background(), args)
		if err != nil {
			return false, err
		}

		// If their reported term is higher then ours, then we
		// aren't leader. So demote to follower, and cancel this
		// replication attempt.
		if resp.Term > currentTerm {
			log.Println("node had higher term, falling back to follower")
			r.stateTransition <- Follower
			return false, nil
		}
		if !resp.Success {
			log.Println("unable to replicate entry: ", nextIndex)
			r.NextIndex[int(nodeId)] -= 1
		} else {
			log.Printf("replicated entry %d\n", nextIndex)

			r.NextIndex[int(nodeId)] += 1
			if r.NextIndex[int(nodeId)] == logLength+1 {
				log.Printf("node %d is fully caught up\n", nodeId)
				caughtUp = true
			}
		}
	}

	return true, nil
}

func main() {
	// Seed the prng.
	rand.Seed(time.Now().Unix())

	flag.Parse()

	// Redirect the output of grpcLog to a file instead of stdout
	grpcLogFile, _ := os.OpenFile(fmt.Sprintf("server%d.log", *serverID), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	newLogger := log.New(grpcLogFile, "[grpclog] ", 0)
	grpclog.SetLogger(newLogger)

	raftServer, err := newRaft(uint32(*serverID))
	if err != nil {
		log.Fatalf("unable to create raft server: %v", err)
	}

	if err := raftServer.Start(); err != nil {
		log.Fatalf("unable to start raft server: %v", err)
	}

	// Grab the port we should listen to.
	selfPort := raftServer.Port
	*port, _ = strconv.Atoi(selfPort)

	fmt.Printf("\nStarting server with ID %d on port %d\n", *serverID, *port)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	raft.RegisterRaftServer(grpcServer, raftServer)

	grpcServer.Serve(lis)
}
