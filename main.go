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
	"google.golang.org/grpc/grpclog"
)

var allServers map[int]string
var serverToIdMap map[string]int
var conn *grpc.ClientConn
var client raft.RaftClient
var connectedToServer uint32
var totalServers int

func main() {
	fmt.Println("Welcome to Raft by Syncinators.\nType 'help' for a list of supported commands.\n")

	fmt.Println("Initializing available servers. Please wait...")

	var err error

	allServers, err = config.GetServersFromConfig()
	if err != nil {
		os.Exit(1)
	}
	totalServers = len(allServers)

	serverToIdMap = make(map[string]int)
	for servId, servIp := range allServers {
		serverToIdMap[servIp] = servId
	}

	fmt.Println("Servers initialized successfully.")
	listServers(allServers)

	// Redirect the output of grpcLog to a file instead of stdout
	grpcLogFile, _ := os.OpenFile(fmt.Sprintf("client.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	newLogger := log.New(grpcLogFile, "[grpclog] ", 0)
	grpclog.SetLogger(newLogger)

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
				return
			}

			connect(serverId)

		case "disconnect":
			disconnect()

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
				connect(int(connectedToServer+1) % totalServers)
			}
			message := command[5:len(command)]
			args := &raft.PostArgs{
				Msg: message,
			}
			reply, err := client.Post(context.Background(), args)
			if err != nil {
				log.Println("Server error:", err)
			}
			// The requested server is not the leader. Need to connect to the leader
			if !reply.Response.Success && reply.Response.Message == "I'm not the leader" {
				fmt.Println("The contacted server is not the leader. Will try to connect to the leader...")
				connect(int(reply.Response.LeaderId))
				reply, err = client.Post(context.Background(), args)
				if err != nil {
					log.Println("Server error:", err)
				}
			}
			fmt.Printf("Server replied: \"%s\"\n", reply.Response.Message)

		case "lookup":
			if !checkConnection(false, true) {
				break
			}
			if getConnectionState() != grpc.Ready {
				fmt.Println("The server connected to is not responding.")
				connect(int(connectedToServer+1) % totalServers)
			}
			args := &raft.LookupArgs{}

			reply, err := client.Lookup(context.Background(), args)
			if err != nil {
				log.Fatal("Server error:", err)
			}
			// The requested server is not the leader. Need to connect to the leader
			if !reply.Response.Success && reply.Response.Message == "I'm not the leader" {
				fmt.Println("The contacted server is not the leader. Will try to connect to the leader...")
				connect(int(reply.Response.LeaderId))
				reply, err = client.Lookup(context.Background(), args)
				if err != nil {
					log.Println("Server error:", err)
				}
			}
			fmt.Println("Total number of messages in server: ", len(reply.Entries))
			printLogMessages(reply.Entries)

		case "config":
			if !checkConnection(false, true) {
				break
			}
			if getConnectionState() != grpc.Ready {
				fmt.Println("The server connected to is not responding.")
				connect(int(connectedToServer+1) % totalServers)
			}

			serversList := strings.Split(input[2], ",")

			args := &raft.ConfigArgs{
				Servers: serversList,
			}

			reply, err := client.Config(context.Background(), args)
			if err != nil {
				log.Fatal("Server error:", err)
			}
			// The requested server is not the leader. Need to connect to the leader
			if !reply.Response.Success && reply.Response.Message == "I'm not the leader" {
				fmt.Println("The contacted server is not the leader. Will try to connect to the leader...")
				connect(int(reply.Response.LeaderId))
				reply, err = client.Config(context.Background(), args)
				if err != nil {
					log.Println("Server error:", err)
				}
			}
			fmt.Printf("Reply from server: \"%s\"\n", reply.Response.Message)
			if reply.Response.Success {
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
			if !value.ConfigChange.Complete {
				fmt.Printf("Log Index: %d, Term: %d, Message: Config change started\n", i, value.Term)
			} else {
				fmt.Printf("Log Index: %d, Term: %d, Message: Config change complete\n", i, value.Term)
			}
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

func connect(serverId int) {
	var err error

	// Disconnect from any previously connected servers
	if conn != nil {
		conn.Close()
	}
	client = nil

	fmt.Printf("Connecting to server %d at %s, please wait...\n", serverId, allServers[serverId])
	targetServer := allServers[serverId]

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())
	opts = append(opts, grpc.WithTimeout(time.Second))
	conn, err = grpc.Dial(targetServer, opts...)
	// Assumption is that at least one server is up, else this will end up in an loop until one of the server comes up
	if err != nil {
		fmt.Println("The server is not responding and maybe down...")
		if conn != nil {
			conn.Close()
		}
		client = nil
	}

	if err != nil {
		flag := false
		for flag == false {
			serverId++
			serverId = serverId % totalServers
			if _, val := allServers[serverId]; val {
				flag = true
			}
		}
		fmt.Println("Trying to connect to another server...")
		connect(serverId)
	} else {
		client = raft.NewRaftClient(conn)
		connectedToServer = uint32(serverId)
		fmt.Printf("Connected to Server %d\n", serverId)
	}
}

func disconnect() {
	connected := checkConnection(false, true)
	if connected {
		conn.Close()
		client = nil
		fmt.Printf("Disconnected from Server %d\n", connectedToServer)
	}
}
