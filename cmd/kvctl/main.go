package main

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"strings"

	"github.com/dines/distributed-kv/raft"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: kvctl <address>")
		fmt.Println("Example: kvctl localhost:9001")
		os.Exit(1)
	}

	address := os.Args[1]
	fmt.Printf("Connecting to %s...\n", address)

	client, err := rpc.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	fmt.Println("Connected! Commands: SET <key> <value>, GET <key>, DELETE <key>, STATUS, QUIT")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		command := strings.ToUpper(parts[0])

		switch command {
		case "QUIT", "EXIT":
			fmt.Println("Bye!")
			return

		case "SET":
			if len(parts) < 3 {
				fmt.Println("Usage: SET <key> <value>")
				continue
			}
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			cmd := raft.Command{Op: "SET", Key: key, Value: value}
			var reply raft.ClientResponse
			err := client.Call("KVServer.SubmitCommand", cmd, &reply)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else if reply.Success {
				fmt.Println("OK")
			} else {
				fmt.Printf("Error: %s\n", reply.Error)
			}

		case "GET":
			if len(parts) < 2 {
				fmt.Println("Usage: GET <key>")
				continue
			}
			key := parts[1]
			cmd := raft.Command{Op: "GET", Key: key}
			var reply raft.ClientResponse
			err := client.Call("KVServer.SubmitCommand", cmd, &reply)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else if reply.Success {
				if reply.Value == "" {
					fmt.Println("(nil)")
				} else {
					fmt.Println(reply.Value)
				}
			} else {
				fmt.Printf("Error: %s\n", reply.Error)
			}

		case "DELETE":
			if len(parts) < 2 {
				fmt.Println("Usage: DELETE <key>")
				continue
			}
			key := parts[1]
			cmd := raft.Command{Op: "DELETE", Key: key}
			var reply raft.ClientResponse
			err := client.Call("KVServer.SubmitCommand", cmd, &reply)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else if reply.Success {
				fmt.Println("OK")
			} else {
				fmt.Printf("Error: %s\n", reply.Error)
			}

		case "STATUS":
			fmt.Println("STATUS command not yet implemented")

		default:
			fmt.Printf("Unknown command: %s\n", command)
			fmt.Println("Commands: SET, GET, DELETE, STATUS, QUIT")
		}
	}
}
