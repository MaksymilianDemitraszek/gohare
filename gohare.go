package gohare

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
)

type Rabbit struct {
	connection *amqp.Connection
	channel *amqp.Channel
	queue *amqp.Queue
	handlers map[string]Handler
	exchangeName string
}

func NewHare(url string, queueName string) *Rabbit{
	rabbit := Rabbit{
		connection:connectAmpq(url),
	}
	rabbit.channel = connectChannel(rabbit.connection)
	rabbit.exchangeName = "main"
	rabbit.declareExchange("main")
	rabbit.declareQueue(queueName)
	rabbit.handlers = make(map[string]Handler)
	return &rabbit
}

type Handler struct {
	handlerFunction func(event *Event)
	rpc bool
}

func (r *Rabbit)declareExchange(name string){
	err := r.channel.ExchangeDeclare(
		name, // name
		"direct",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare an exchange")
}

func (r *Rabbit)declareQueue(name string) {
	q,err := r.channel.QueueDeclare(
		name, // name
		false,       // durableW
		false,       // delete when usused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")
	r.queue = &q

	err = r.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")
}

func (r *Rabbit)Subscribe(routingKey string, handler func(event *Event), rpc bool){
	err := r.channel.QueueBind(
		r.queue.Name,        // queue name
		routingKey,             // routing key
		r.exchangeName, // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")
	r.handlers[routingKey] = Handler{
		handler,
		rpc,
	}
}

func (r *Rabbit)Listen(){
	msgs, err := r.channel.Consume(
		r.queue.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-waitW
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for message := range msgs {
			fmt.Println("Got event "+ message.RoutingKey)
			event := Event{
				Message:message,
			}
			r.handlers[message.RoutingKey].handlerFunction(&event)

			if r.handlers[message.RoutingKey].rpc{
				err = r.respond(event)
			}

			message.Ack(false)
		}
	}()
	log.Printf("Consumer is runnning")
	<-forever
}

type Event struct{
	Message amqp.Delivery
	response []byte
	isResponseMade bool
	isError bool
}

func(eve *Event)Error(errorMessage string){
	errorResponse := ErrorForm{
		ErrorMessage:errorMessage,
	}
	eve.isError = true
	eve.MakeResponse(&errorResponse)
}

func(eve *Event)MakeResponse(response interface{}){
	if eve.isResponseMade == false{
		responseJson, err := json.Marshal(&response)
		if err != nil{
			panic(err.Error())
		}
		eve.response = responseJson
		eve.isResponseMade = true
	}
}

func(eve *Event)GetResponse(response interface{})[]byte{
	return eve.response
}

func (r *Rabbit)Send(routingKey string, messageBody []byte){
	err := r.channel.Publish(
		r.exchangeName, // exchange
		routingKey, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:   "application/json",
			Body:          messageBody,
		})
	if err != nil{
		panic(err.Error())
	}
}

func(r *Rabbit) declareExclusiveQueue()*amqp.Queue{
	q, err := r.channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to register a consumer")
	return &q
}

func (r *Rabbit) loadResponseListener(queue *amqp.Queue) <-chan amqp.Delivery {
	msgs, err := r.channel.Consume(
		queue.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
	return msgs
}

type RequestForm struct{
	RoutingKey string
	MessageBody []byte
	CorrelationId string
	Response []byte
	IsResponseError bool
}

func NewRequest(routingKey string, messageBody []byte) *RequestForm{
	rq := RequestForm{}
	rq.RoutingKey = routingKey
	rq.MessageBody = messageBody
	return &rq
}

func (r *Rabbit)Request(requests []*RequestForm){
	queue := r.declareExclusiveQueue()
	msgs := r.loadResponseListener(queue)


	for i:=0; i<len(requests); i++ {
		requests[i].CorrelationId = randomString(32)

		err := r.channel.Publish(
			r.exchangeName,          // exchange
			requests[i].RoutingKey, // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: requests[i].CorrelationId,
				ReplyTo:       queue.Name,
				Body: requests[i].MessageBody,

			})
		failOnError(err, "Failed to publish a message")

	}


	requestsCount := 0
	for d := range msgs {
		rq, err := findRequest(d.CorrelationId, requests)
		if err == nil {
			if val, ok := d.Headers["error"]; ok {
				if val, ok := val.(bool); ok {
					rq.Response = d.Body
					rq.IsResponseError = val
				}
			}else{
				rq.Response = []byte("Service error")
				rq.IsResponseError = true
			}

		}
		requestsCount++
		if requestsCount == len(requests){
			return
		}
	}
}

func findRequest(corrId string, requests []*RequestForm) (*RequestForm, error){
	for i:=0; i<len(requests); i++ {
		if requests[i].CorrelationId == corrId{
			return requests[i], nil
		}
	}
	return &RequestForm{}, errors.New(("Request does't exist"))
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func (r *Rabbit)respond(event Event)error{

	headers := make(map[string]interface{})
	headers["error"] = event.isError
	err := r.channel.Publish(
		"",        // exchange
		event.Message.ReplyTo, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:   "application/json",
			CorrelationId: event.Message.CorrelationId,
			Body:          event.response,
			Headers: headers,
		})
	return err
}

func connectChannel(connection *amqp.Connection) *amqp.Channel{
	ch, err := connection.Channel()
	failOnError(err, "Failed to open a channel")
	return ch
}

func connectAmpq(url string) *amqp.Connection{
	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")
	return conn
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func JsonToStruct(rawResponse []byte ,jsonResponse interface{}){
	if err := json.Unmarshal(rawResponse, &jsonResponse); err != nil{
		fmt.Println(err.Error())
	}
}

type ErrorForm struct {
	ErrorMessage string `json:"error"`
}

func MakeErrorResponse(message string)(ErrorForm, bool){
	errorForm := ErrorForm{
		ErrorMessage:message,
	}
	return errorForm, true
}
