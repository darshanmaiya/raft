package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

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
	// Persistent state on all servers:
	//
	// (Updated on stable storage before responding to RPCs)
	ServerID uint32
	Port     string
	State    NodeState

	// CurrentTerm is the latest term server has seen (initialized to 0 on
	// first boot, increases monotonically)
	CurrentTerm uint32

	// VotedFor is the candidateId that received vote in current term (or
	// null if none)
	VotedFor *uint32

	// Log is all log entries. Each entry contains command for state
	// machine, and term when entry was received by leader (first index is 1)
	Log []*raft.LogEntry

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
	// to that server (initialized to leader	last log index + 1)
	NextIndex map[int]uint32

	// MatchIndex marks for each server, index of highest log entry known
	// to be replicated on server (initialized to 0, increases monotonically)
	MatchIndex map[int]uint32

	// Participants is the set of all participating servers in Raft.
	Participants map[int]string

	stateTransition chan NodeState

	electionLost chan struct{}
	msgReceived  chan struct{}

	nodes map[uint32]raft.RaftClient

	wg   sync.WaitGroup
	quit chan struct{}
}

// newRaft creates a new instance of raft given its server id.
func newRaft(serverID uint32) (*Raft, error) {
	s := &Raft{
		ServerID:     serverID,
		State:        Follower,
		Participants: make(map[int]string),

		NextIndex:  make(map[int]uint32),
		MatchIndex: make(map[int]uint32),

		stateTransition: make(chan NodeState, 1),

		electionLost: make(chan struct{}, 1),
		msgReceived:  make(chan struct{}, 1),

		nodes: make(map[uint32]raft.RaftClient),

		quit: make(chan struct{}, 1),
	}

	// Get the map of participants from the global config.
	var err error
	s.Participants, err = config.GetServersFromConfig()
	if err != nil {
		return nil, err
	}

	// Set the port for this server
	s.Port = strings.Split(s.Participants[int(serverID)], ":")[1]

	for i, server := range s.Participants {
		if i == int(s.ServerID) {
			continue
		}

		var opts []grpc.DialOption
		// TODO(roasbeef): add goroutine to poll state of all
		// connections and reconnect (will need mutex).
		opts = append(opts, grpc.WithInsecure())
		opts = append(opts, grpc.WithTimeout(time.Minute*5))
		conn, err := grpc.Dial(server, opts...)
		if err != nil {
			return nil, err
		}

		client := raft.NewRaftClient(conn)
		s.nodes[uint32(i)] = client
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
	return &raft.PostResponse{}, nil
}

func (s *Raft) Lookup(ctx context.Context, req *raft.LookupArgs) (*raft.LookupResponse, error) {

	return &raft.LookupResponse{}, nil
}

func (s *Raft) RequestVote(ctx context.Context, req *raft.RequestVoteArgs) (*raft.RequestVoteResponse, error) {
	var granted bool

	// If we've already voted for a new candidate during this term, then
	// decline the request, otherwise accept this as a new candidate and
	// record our vote.
	if req.Term < s.CurrentTerm || s.VotedFor != nil {
		granted = false
	} else {
		s.VotedFor = &req.CandidateId
		granted = true
	}

	fmt.Printf("Server %d is requesting vote..\n", req.CandidateId)
	return &raft.RequestVoteResponse{
		Term:        s.CurrentTerm,
		VoteGranted: granted,
	}, nil
}

func (s *Raft) AppendEntries(ctx context.Context, req *raft.AppendEntriesArgs) (*raft.AppendEntriesResponse, error) {
	// If we get a heart beat with an empty log and we're a candidate, then we
	// lost the election.
	if s.State == Candidate && len(req.Entries) == 0 {
		s.electionLost <- struct{}{}
	}

	s.msgReceived <- struct{}{}

	return &raft.AppendEntriesResponse{}, nil
}

func (s *Raft) printParticipants() {
	fmt.Println("Available servers are:\n")

	for i, value := range s.Participants {
		if i == int(s.ServerID) {
			fmt.Printf(">> Server %d @ %s <<\n", i, value)
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
	var serverDownTimer <-chan time.Time
	var electionTimer <-chan time.Time
	var heartBeatTimer <-chan time.Time

	electionCancel := make(chan struct{}, 1)
out:
	for {
		select {
		case <-r.msgReceived:
			// We got a message from the leader before the timeout was
			// up. So reset it.
			serverDownTimer = time.After(serverDownTimeout)
		case <-serverDownTimer:
			// Haven't received message from leader in serverDownTimeout
			// period. So start an election after a random period of time.
			electionBackOff := time.Duration(rand.Intn(140)+10) * time.Millisecond
			electionTimer = time.After(electionBackOff)
		case <-electionTimer:
			r.State = Candidate

			electionTimeout := time.After(serverDownTimeout)
			go r.startElection(electionCancel, electionTimeout, serverDownTimeout)
		case <-r.electionLost:
			r.State = Follower

			// The election was lost, cancel the election timer.
			electionTimer = nil
			serverDownTimer = time.After(serverDownTimeout)
			electionCancel <- struct{}{}
		case r.State = <-r.stateTransition:
			// Reset all the timers, and trigger the heart beat timer.
			electionTimer = nil

			go r.sendHeatBeat()

			heartBeatTimer = time.After(heartBeatTimeout)
		case <-heartBeatTimer:
			// Send out heart beats to all participants, and reset the
			// timer.
			go r.sendHeatBeat()

			heartBeatTimer = time.After(heartBeatTimeout)
		case <-r.quit:
			break out
		}
	}

	r.wg.Done()
}

func (s *Raft) sendHeatBeat() {
	for _, node := range s.nodes {
		go func(n raft.RaftClient) {
			args := &raft.AppendEntriesArgs{
				Term:     s.CurrentTerm,
				LeaderId: s.CurrentTerm,
			}

			if _, err := n.AppendEntries(context.Background(), args); err != nil {
				log.Fatalf("Server during heartbeat error:", err)
			}
		}(node)
	}
}

func (s *Raft) startElection(cancelSignal chan struct{},
	electionTimeout <-chan time.Time, timeoutDuration time.Duration) {

	fmt.Println("Sending RequestVoteRPC...")

	// +1 not required as self vote is always assumed
	majority := len(s.Participants) / 2
	response := make(chan raft.RequestVoteResponse, len(s.Participants))
	successVotes := 0

	for _, node := range s.nodes {
		go func(n raft.RaftClient) {
			term := uint32(0)
			args := &raft.RequestVoteArgs{
				Term:         s.CurrentTerm,
				CandidateId:  s.ServerID,
				LastLogIndex: uint32(len(s.Log)),
			}
			if s.Log != nil {
				term = s.Log[len(s.Log)-1].Term
			}
			args.LastLogTerm = uint32(term)

			reply, err := n.RequestVote(context.Background(), args)
			if err != nil {
				log.Fatal("Server error:", err)
				response <- raft.RequestVoteResponse{
					VoteGranted: false,
				}
			} else {
				response <- raft.RequestVoteResponse{
					Term:        reply.Term,
					VoteGranted: reply.VoteGranted,
				}
			}
		}(node)
	}

	for i := 0; i < len(s.Participants); i++ {
		select {
		case <-cancelSignal:
			return
		case <-electionTimeout:
			go s.startElection(cancelSignal, time.After(timeoutDuration), timeoutDuration)
			return
		case <-response:
			reply := <-response
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

func main() {
	// Seed the prng.
	rand.Seed(time.Now().Unix())

	flag.Parse()

	raftServer, err := newRaft(uint32(*serverID))
	if err != nil {
		log.Fatalf("unable to create raft server: %v", err)
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
