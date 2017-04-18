package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"sawtooth_sdk/protobuf"
)

type TransactionProcessor struct {
	Url string
	Handlers []TransactionHandler
	socket *zmq.Socket
}


type TransactionHandler interface {

}

type IntkeyTransactionHandler struct {
	NamespacePrefix string
}

func (p *TransactionProcessor) buildRegisterMessages() [][]byte {
	// Send registration request
	request := &protobuf.TpRegisterRequest{
		Family:     "test_family",
		Version:    "1.0",
		Encoding:   "protobuf",
		Namespaces: []string{"test_namespace"},
	}
	requestData, err := proto.Marshal(request)
	if err != nil {
		fmt.Printf("Failed to marshal: %v\n", err)
	}

	correlationId := "123"
	msg := &protobuf.Message{
		MessageType: protobuf.Message_TP_REGISTER_REQUEST,
		CorrelationId: correlationId,
		Content: requestData,
	}

	msgData, err := proto.Marshal(msg)
	if err != nil {
		fmt.Printf("Failed to marshal: %v\n", err)
	}

	return [][]byte{msgData}
}

func (p *TransactionProcessor) register() {
	ch := make(chan int)
	count := 0
	for _, msg := range p.buildRegisterMessages() {
		go func() {
			bytes, err := p.socket.SendBytes(msg, 0)
		    if err != nil {
				fmt.Printf("Failed to send message: %v\n", err)
		    }
		    ch <- bytes
		}()
		count++
	}

	for i := 0; i < count; i++ {
		bytes := <-ch
		fmt.Printf("sent %v bytes\n", bytes)
	}
}

func (p *TransactionProcessor) Start() {
	identity := "id"

	context, _ := zmq.NewContext()
	socket, _ := context.NewSocket(zmq.REQ)
	defer context.Term()
	defer socket.Close()

	socket.SetIdentity(identity)
	p.socket = socket

	fmt.Printf("Connecting to %v...", p.Url)
	socket.Connect(p.Url)
	fmt.Println("done")

	p.register()

	// Wait for response
	data, _ := socket.RecvBytes(0)
	reply := &protobuf.TpRegisterResponse{}
	err := proto.Unmarshal(data, reply)
	if err != nil {
		fmt.Printf("Failed to unmarshal")
	}

	fmt.Printf("Register Response: %v\n", reply.Status)
}

func main() {
	endpoint := "tcp://127.0.0.1:40000"
	intkeyPrefix := "xxx"

	handler := IntkeyTransactionHandler{
		NamespacePrefix: intkeyPrefix,
	}

	processor := TransactionProcessor{
		Url: endpoint,
		Handlers: []TransactionHandler{handler},
	}

	processor.Start()
}
