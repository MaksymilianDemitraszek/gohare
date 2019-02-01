package gohare

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var RabClient = NewHare("amqp://test:test@localhost:5672/", "client")
var RabServer = NewHare("amqp://test:test@localhost:5672/", "server")
var RabServerSlower = NewHare("amqp://test:test@localhost:5672/", "serverSlower")
func init(){
	RabServer.Subscribe(
		"testNoRpc",
		func (event *Event){
			request := mockMessage{}
			err := json.Unmarshal(event.Message.Body, &request)
			if err != nil{
				panic("Message error!")
			}
		},
		false)

	RabServer.Subscribe(
		"testRpc",
		func (event *Event){
			request := mockMessage{}
			err := json.Unmarshal(event.Message.Body, &request)
			if err != nil{
				panic("Message error!")
			}
			event.MakeResponse(&request)
			return
		},
		true)

	RabServerSlower.Subscribe("testSlowRpc",
		func (event *Event){
			time.Sleep(2 * time.Second) //simulates longer task
			request := mockMessage{}
			err := json.Unmarshal(event.Message.Body, &request)
			if err != nil{
				panic("Message error!")
			}
			event.MakeResponse(&request)
			return
		},
		true)
	go RabServer.Listen()
	go RabServerSlower.Listen()
}


func TestNoRpc(t *testing.T){


	requestBody := StructToJson(&mockMessage{
		Message: "message",
	})

	RabClient.Publish("testNoRpc", requestBody)
}

func TestRpc(t *testing.T){

	requestBody := StructToJson(&mockMessage{
		Message: "message",
	})
	req := []*RequestForm{
		NewRequest("testRpc", requestBody),
	}

	RabClient.Request(req)

	resp := mockMessage{}
	err := json.Unmarshal(req[0].MessageBody, &resp)
	assert.Nil(t, err, "Response parsing error")


	assert.Equal(t, resp.Message, "message")
}


func TestMultipleRpc(t *testing.T){
	requestBody := StructToJson(&mockMessage{
		Message: "message",
	})
	requestBody2 := StructToJson(&mockMessage{
		Message: "slowerMessage",
	})

	req := []*RequestForm{
		NewRequest("testSlowRpc", requestBody2),
		NewRequest("testRpc", requestBody),

	}

	RabClient.Request(req)

	resp := mockMessage{}
	for i:=0; i<len(req); i++ {
		err := json.Unmarshal(req[i].Response, &resp)
		assert.Nil(t, err, "Response parsing error")

		switch req[i].RoutingKey {
			case "testSlowRpc":
				assert.Equal(t, resp.Message, "slowerMessage")
			case "testRpc":
				assert.Equal(t, resp.Message, "message")
			default:
				fmt.Println("Error of routing key")
		}
	}
}
type mockMessage struct {
	Message string `json:"test"`
}


func StructToJson(request interface{})[]byte{
	reqBody, err := json.Marshal(request)
	if err != nil{
		fmt.Println(err.Error())
	}
	return reqBody
}