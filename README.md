# Raft
Raft protocol implementation in Go

## Running the project

Note: This project requires the Go environment to be setup prior to following the instructions below.
Please check here: https://golang.org/doc/install on instructions on how to setup and configure Go on your system.

1: go get github.com/darshanmaiya/raft
This will download the code, build and install the project.

2: cd $GoPath/src/github.com/darshanmaiya/raft
This will change the directory to the one containing project executables

3: Edit config/config.json with the appropriate server IPs and ports. (Please follow the exisiting format in the file. Ports are mandatory)

4: Run the client by executing the following command: ./raft

5: cd server

6: Run the server by executing the following command: ./server --id=x (where x denotes an id specified in config.json)
Repeat this for starting all the server by specifying different ids

## Config change
For any server added later via config change and not specified in the config.json, will be started on port 50000 + id
