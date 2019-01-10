package go_rabbit

import (
	"encoding/json"
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
	rabbit.exchangeName = ""
	rabbit.declareExchange("")
	rabbit.declareQueue(queueName)
	rabbit.handlers = make(map[string]Handler)
	return &rabbit
}

type Handler struct {
	handlerFunction func(amqp.Delivery)(interface{}, bool)
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

func (r *Rabbit)Subscribe(routingKey string, handler func(amqp.Delivery)(interface{}, bool), rpc bool){
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
			fmt.Println("Got routing key "+ message.RoutingKey)
			if r.handlers[message.RoutingKey].rpc{
				response, isError := r.handlers[message.RoutingKey].handlerFunction(message)
				err = r.respond(message, response, isError)
			}else {
				r.handlers[message.RoutingKey].handlerFunction(message)
			}
			message.Ack(false)
		}
	}()
	log.Printf("Consumer is runnning")
	<-forever
}

func (r *Rabbit)Send(routingKey string, messageBody interface{})error{
	responseJson, _ := json.Marshal(messageBody)
	err := r.channel.Publish(
		"",        // exchange
		routingKey, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:   "application/json",
			Body:          responseJson,
		})
	return err
}

func (r *Rabbit)Request(routingKey string, messageBody []byte)([]byte, bool){
	//responseJson, _ := json.Marshal(messageBody)

	msgs, err := r.channel.Consume(
		r.queue.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	corrId := randomString(32)

	err = r.channel.Publish(
		"main",          // exchange
		routingKey, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: corrId,
			ReplyTo:       r.queue.Name,
			Body: messageBody,

		})
	failOnError(err, "Failed to publish a message")

	for d := range msgs {
		if corrId == d.CorrelationId {
			if val, ok := d.Headers["error"]; ok {
				if val, ok := val.(bool); ok {
					return d.Body, val
				}

			}
			return []byte("Service error"), true
		}
	}
	return []byte("Service error"), true
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

func (r *Rabbit)respond(message amqp.Delivery, response interface{}, isError bool)error{
	responseJson, _ := json.Marshal(response)

	headers := make(map[string]interface{})
	headers["error"] = isError
	err := r.channel.Publish(
		"",        // exchange
		message.ReplyTo, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:   "application/json",
			CorrelationId: message.CorrelationId,
			Body:          responseJson,
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
