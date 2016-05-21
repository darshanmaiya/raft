package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/darshanmaiya/raft/config"
	"github.com/darshanmaiya/raft/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

const (
	FOLLOWER  = iota // FOLLOWER == 0
	CANDIDATE = iota // CANDIDATE == 1
	LEADER    = iota // LEADER == 2

	ELECTION_TIMEOUT = 150 // in ms
)

// Raft represents an instance of raft implementing the gRPC
// server interface.
type Raft struct {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	ServerID              uint32
	Port                  string
	State                 int
	LastMessageTimeStamp  int64
	MajorityVotesReceived bool

	CurrentTerm uint32           // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VotedFor    uint32           // candidateId that received vote in current term (or null if none)
	Log         []*raft.LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	CommitIndex uint32 // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	LastApplied uint32 // index of highest log entry applied to state	machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	// (Reinitialized after election)
	NextIndex  []uint32 // for each server, index of the next log entry to send to that server (initialized to leader	last log index + 1)
	MatchIndex []uint32 // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	Participants map[int]string // All participating servers in Raft
}

// newRaft creates a new instance of raft given its server id.
func newRaft(serverID uint32) (*Raft, error) {
	s := &Raft{
		ServerID:     serverID,
		State:        FOLLOWER,
		Participants: make(map[int]string),
	}

	// Get the map of participants from the global config.
	var err error
	s.Participants, err = config.GetServersFromConfig()
	if err != nil {
		return nil, err
	}

	// Set the port for this server
	s.Port = strings.Split(s.Participants[int(serverID)], ":")[1]

	fmt.Println("Participant servers initialized successfully.")
	s.printParticipants()
	return s, nil
}

func (s *Raft) Post(ctx context.Context, req *raft.PostArgs) (*raft.PostResponse, error) {
	return &raft.PostResponse{}, nil
}

func (s *Raft) Lookup(ctx context.Context, req *raft.LookupArgs) (*raft.LookupResponse, error) {

	return &raft.LookupResponse{}, nil
}

func (s *Raft) RequestVote(ctx context.Context, req *raft.RequestVoteArgs) (*raft.RequestVoteResponse, error) {

	fmt.Printf("Server %d is requesting vote..\n", req.CandidateId)
	s.LastMessageTimeStamp = time.Now().UnixNano()
	return &raft.RequestVoteResponse{
		Term:        req.Term,
		VoteGranted: true,
	}, nil
}

func (s *Raft) AppendEntries(ctx context.Context, req *raft.AppendEntriesArgs) (*raft.AppendEntriesResponse, error) {

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

func (s *Raft) sendRequestRPC() {
	fmt.Println("Sending RequestVoteRPC...")
	majority := len(s.Participants) / 2 // +1 not required as self vote is always assumed
	response := make(chan raft.RequestVoteResponse, len(s.Participants))
	successVotes := 0

	for i, value := range s.Participants {
		if i != int(s.ServerID) {
			var opts []grpc.DialOption
			opts = append(opts, grpc.WithInsecure())
			conn, err := grpc.Dial(value, opts...)
			if err != nil {
				grpclog.Fatalf("Failed to dial: %v", err)
				break
			}
			go func() {
				client := raft.NewRaftClient(conn)
				args := &raft.RequestVoteArgs{
					Term:         s.CurrentTerm,
					CandidateId:  s.ServerID,
					LastLogIndex: 0,
					LastLogTerm:  0,
				}

				reply, err := client.RequestVote(context.Background(), args)
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

				defer conn.Close()
			}()
		}
	}

	for i := 0; i < len(s.Participants); i++ {
		reply := <-response
		if reply.VoteGranted {
			successVotes++
		}

		if successVotes >= majority {
			s.MajorityVotesReceived = true
			break
		}
	}
}

func (s *Raft) sendHeartBeat() {
	fmt.Println("Sending HeartBeatRPC...")
	majority := len(s.Participants) / 2 // +1 not required as self vote is always assumed
	response := make(chan raft.AppendEntryResponse, len(s.Participants))
	successVotes := 0

	for i, value := range s.Participants {
		if i != int(s.ServerID) {
			var opts []grpc.DialOption
			opts = append(opts, grpc.WithInsecure())
			conn, err := grpc.Dial(value, opts...)
			if err != nil {
				grpclog.Fatalf("Failed to dial: %v", err)
				break
			}
			go func() {
				client := raft.NewRaftClient(conn)
				args := &raft.RequestVoteArgs{
					Term:         s.CurrentTerm,
					CandidateId:  s.ServerID,
					LastLogIndex: 0,
					LastLogTerm:  0,
				}

				reply, err := client.RequestVote(context.Background(), args)
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

				defer conn.Close()
			}()
		}
	}

	for i := 0; i < len(s.Participants); i++ {
		reply := <-response
		if reply.VoteGranted {
			successVotes++
		}

		if successVotes >= majority {
			s.MajorityVotesReceived = true
			break
		}
	}
}

func (s *Raft) timeout() {
	if s.State == FOLLOWER && time.Now().UnixNano()-s.LastMessageTimeStamp > ELECTION_TIMEOUT*1000000 { // TODO Remove 100000
		fmt.Println("No message from leader in the specified timeout. Transitioning to CANDIDATE...")
		s.State = CANDIDATE
		s.MajorityVotesReceived = false
		s.sendRequestRPC()
	} else if s.State == CANDIDATE {
		// if enough votes received
		s.MajorityVotesReceived = true
		if s.MajorityVotesReceived {
			fmt.Println("Votes received to become leader. Transitioning to LEADER...")
			s.State = LEADER
			fmt.Println("I'm now a leader...")
		} else {
			s.State = FOLLOWER
		}
	} else if s.State == LEADER {
		s.sendHeartBeat()
	}
	time.Sleep(ELECTION_TIMEOUT / 150 * time.Second) // TODO Remove 150
	s.timeout()
}

func main() {
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

	go grpcServer.Serve(lis)

	raftServer.LastMessageTimeStamp = time.Now().UnixNano()
	go raftServer.timeout()

	for {

	}
}
