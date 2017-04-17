package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"sawtooth_sdk/protobuf"
)

func main() {
	context, _ := zmq.NewContext()
	socket, _ := context.NewSocket(zmq.REQ)
	defer context.Term()
	defer socket.Close()

	endpoint := "tcp://localhost:40000"
	fmt.Printf("Connecting to %v...", endpoint)
	socket.Connect(endpoint)
	fmt.Println("done")

	// Send registration request
	msg := &protobuf.TpRegisterRequest{
		Family:     "test_family",
		Version:    "1.0",
		Encoding:   "protobuf",
		Namespaces: []string{"test_namespace"},
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		fmt.Printf("Failed to marshal")
	}

	socket.SendBytes(data, 0)

	// Wait for response
	data, _ = socket.RecvBytes(0)
	reply := &protobuf.TpRegisterResponse{}
	err = proto.Unmarshal(data, reply)
	if err != nil {
		fmt.Printf("Failed to unmarshal")
	}

	fmt.Printf("Register Response: %v\n", reply.Status)
}
