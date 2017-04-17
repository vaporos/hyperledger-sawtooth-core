package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"sawtooth_sdk/protobuf"
)

func main() {
	context, _ := zmq.NewContext()
	socket, _ := context.NewSocket(zmq.REP)
	defer context.Term()
	defer socket.Close()

	endpoint := "tcp://127.0.0.1:40000"
	fmt.Printf("Binding to %v...", endpoint)
	socket.Bind(endpoint)
	fmt.Println("done")

	// Wait for registration request
	data, _ := socket.RecvBytes(0)

	msg := &protobuf.TpRegisterRequest{}
	err := proto.Unmarshal(data, msg)
	if err != nil {
		fmt.Printf("Failed to unmarshal")
	}

	fmt.Printf(
		"Processor registerd: %v, %v, %v, %v\n",
		msg.Family, msg.Version, msg.Encoding, msg.Namespaces,
	)

	// Send reply back to processor
	reply := &protobuf.TpRegisterResponse{
		Status: protobuf.TpRegisterResponse_OK,
	}
	data, err = proto.Marshal(reply)
	if err != nil {
		fmt.Printf("Failed to marshal")
	}

	socket.SendBytes(data, 0)
}
