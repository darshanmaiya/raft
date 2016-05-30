package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/darshanmaiya/raft/config"
	"github.com/darshanmaiya/raft/protos"
)

var allServers map[int]string
var serverToIdMap map[string]int
var conn *grpc.ClientConn
var client raft.RaftClient
var connectedToServer uint32

func main() {
	fmt.Println("Welcome to Raft by Syncinators.\nType 'help' for a list of supported commands.\n")

	fmt.Println("Initializing available servers. Please wait...")

	var err error

	allServers, err = config.GetServersFromConfig()
	if err != nil {
		os.Exit(1)
	}

	serverToIdMap = make(map[string]int)
	for servId, servIp := range allServers {
		serverToIdMap[servIp] = servId
	}

	fmt.Println("Servers initialized successfully.")
	listServers(allServers)

	for {
		consoleReader := bufio.NewReader(os.Stdin)

		fmt.Println("\nEnter command: ")
		command, _ := consoleReader.ReadString('\n')

		command = command[0 : len(command)-1]

		//fmt.Println("\nCommand: " + command)

		input := strings.Split(command, " ")

		switch strings.ToLower(input[0]) {
		case "":

		case "connect":
			var serverId int
			serverId, err = strconv.Atoi(input[1])
			if err != nil {
				fmt.Println("Please pass an integer denoting server ID to connect to.")
				break
			}
			fmt.Printf("Connecting to server %d at %s, please wait...\n", serverId, allServers[serverId])
			targetServer := allServers[serverId]

			var opts []grpc.DialOption
			opts = append(opts, grpc.WithInsecure())
			opts = append(opts, grpc.WithBlock())
			opts = append(opts, grpc.WithTimeout(time.Second))
			conn, err = grpc.Dial(targetServer, opts...)
			if err != nil {
				fmt.Println("The server to is not responding and maybe down...\nPlease retry after some time")
				if conn != nil {
					conn.Close()
				}
				client = nil
				break
			}

			client = raft.NewRaftClient(conn)
			connectedToServer = uint32(serverId)
			fmt.Printf("Connected to Server %d\n", serverId)

		case "disconnect":
			connected := checkConnection(false, true)
			if connected {
				conn.Close()
				client = nil
				fmt.Printf("Disconnected from Server %d\n", connectedToServer)
			}

		case "list":
			listServers(allServers)

		case "which":
			checkConnection(true, true)

		case "post":
			if !checkConnection(false, true) {
				break
			}
			if getConnectionState() != grpc.Ready {
				fmt.Println("The server connected to is not responding.")
				break
			}
			message := command[5:len(command)]
			args := &raft.PostArgs{
				Msg: message,
			}
			reply, err := client.Post(context.Background(), args)
			if err != nil {
				log.Fatal("Server error:", err)
			}
			fmt.Printf("Server replied: \"%s\"\n", reply.Resp)

		case "lookup":
			if !checkConnection(false, true) {
				break
			}
			if getConnectionState() != grpc.Ready {
				fmt.Println("The server connected to is not responding.")
				break
			}
			args := &raft.LookupArgs{}

			reply, err := client.Lookup(context.Background(), args)
			if err != nil {
				log.Fatal("Server error:", err)
			}

			fmt.Println("Total number of messages in server: ", len(reply.Entries))
			printLogMessages(reply.Entries)

		case "config":
			if !checkConnection(false, true) {
				break
			}
			if getConnectionState() != grpc.Ready {
				fmt.Println("The server connected to is not responding.")
				break
			}

			op := input[1]
			numServers, _ := strconv.Atoi(input[2])
			newServers := make(map[uint32]string)
			if op == "add" {
				for serverIps := 0; serverIps < numServers; serverIps++ {
					newServers[uint32(len(allServers))+uint32(serverIps)] = input[3+serverIps]
				}
			} else if op == "remove" {
				for serverIps := 0; serverIps < numServers; serverIps++ {
					newServers[uint32(serverToIdMap[input[3+serverIps]])] = input[3+serverIps]
				}
			} else {
				for serverIps := 0; serverIps < numServers; serverIps++ {
					newServers[uint32(serverIps)] = input[3+serverIps]
				}
			}

			args := &raft.ConfigArgs{
				NewConfig: &raft.ConfigChange{
					Command: op,
					Servers: newServers,
				},
			}

			reply, err := client.Config(context.Background(), args)
			if err != nil {
				log.Fatal("Server error:", err)
			}

			fmt.Printf("Reply from server: \"%s\"\n", reply.Message)
			if reply.Success {
				allServers = make(map[int]string)
				serverToIdMap = make(map[string]int)
				for servId, servIp := range reply.Servers {
					allServers[int(servId)] = servIp
					serverToIdMap[servIp] = int(servId)
				}
			}

			listServers(allServers)

		default:
			fmt.Printf("Unknown command: %s\n", input[0])
			fallthrough

		case "help":
			fmt.Println("Here's a list of commands supported by the application:")

			fmt.Println("\nConnect commands\n--------------------------------")
			fmt.Println("connect <ID> - Connect to server with given ID")
			fmt.Println("disconnect - Disconnect from the server currently connected to")
			fmt.Println("list - List available servers to connect")
			fmt.Println("which - List details of server currently connected to")

			fmt.Println("\nRaft Client commands\n--------------------------------")
			fmt.Println("post <Message> - Post a message")
			fmt.Println("lookup - Display the posts in server in causal order")
			fmt.Println("config <add/remove/change> <number of servers> <new serverips> - Change config to specified number of nodes")

			fmt.Println("\nMisc. commands\n--------------------------------")
			fmt.Println("help - Display all supported commands")
			fmt.Println("exit - Exit application")

		case "exit":
			fmt.Println("\nRaft client shutting down...\nBye :)")
			os.Exit(0)
		}
	}
}

func listServers(serversList map[int]string) {
	fmt.Println("Available servers are:\n")

	for i, value := range serversList {
		fmt.Printf("Server %d @ %s\n", i, value)
	}
}

func printLogMessages(messages []*raft.LogEntry) {
	for i, value := range messages {
		if value.ConfigChange == nil {
			fmt.Printf("Log Index: %d, Term: %d, Message: \"%s\"\n", i, value.Term, value.Msg)
		} else {
			fmt.Printf("Log Index: %d, Term: %d, Config change: \"%s\"\n", i, value.Term, value.ConfigChange.Command)
		}
	}
}

func checkConnection(printOnConnected bool, printOnNotConnected bool) bool {
	if client == nil {

		if printOnNotConnected {
			fmt.Println("Not connected to any server.")
		}

		return false
	}

	if printOnConnected {
		fmt.Printf("Connected to Server %d\n", connectedToServer)
	}

	return true
}

func getConnectionState() grpc.ConnectivityState {
	state, err := conn.State()
	if err != nil {
		return grpc.TransientFailure
	}
	return state
}
