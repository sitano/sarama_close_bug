package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/satori/go.uuid"
)

// Sarma configuration options
var (
	brokers = ""
	version = ""
	oldest  = true
	verbose = false
)

func init() {
	flag.StringVar(&brokers, "brokers", "", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&version, "version", "2.1.1", "Kafka cluster version")
	flag.BoolVar(&oldest, "oldest", true, "Kafka consumer consume initial ofset from oldest")
	flag.BoolVar(&verbose, "verbose", false, "Sarama logging")
	flag.Parse()

	if len(brokers) == 0 {
		panic("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}
}

func main() {
	log.Println("Starting a new Sarama consumer")

	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		panic(err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = version

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	c, err := sarama.NewClient(strings.Split(brokers, ","), config)
	if err != nil && err != sarama.ErrNoError {
		log.Fatal("client connect", err)
	} else {
		log.Println("client connected")
	}

	for i, b := range c.Brokers() {
		if err = b.Open(c.Config()); err != nil {
			log.Fatal("can't connect to broker", b, "i = ", i)
		}
	}

	// create topic
	topic := uuid.NewV4().String()

	req := &sarama.CreateTopicsRequest{
		TopicDetails: map[string]*sarama.TopicDetail{},
		Timeout:      time.Minute,
	}

	req.TopicDetails[topic] = &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	// topics creates brokers controller
	b, err := c.Controller()
	if err != nil && err != sarama.ErrNoError {
		log.Fatal("can't obtain controller", err)
	}

	ok, err := b.Connected()
	if err != nil && err != sarama.ErrNoError {
		log.Fatal("can't check connected status", err)
	}
	if !ok {
		if err = b.Open(c.Config()); err != sarama.ErrAlreadyConnected {
			log.Fatal("oops", err)
		}
	}

	res, err := b.CreateTopics(req)
	if err != nil && err != sarama.ErrNoError {
		log.Fatal("create topic", err)
	}

	if res.TopicErrors != nil {
		for _, te := range res.TopicErrors {
			if te.Err != sarama.ErrNoError {
				log.Fatal("create topic", te.Err)
			}
		}
	}

	log.Println("new topic with single partition = ", topic)

	/**
	 * Setup a new Sarama consumer group
	 */
	group := uuid.NewV4().String()
	log.Println("new group = ", group)

	consumer1 := Consumer{id: 1, ready: make(chan struct{}, 2)}
	consumer2 := Consumer{id: 2, ready: make(chan struct{}, 2)}

	for _, tmp := range []*Consumer{&consumer1, &consumer2} {
		consumer := tmp

		log.Println("starting ", consumer.id)
		client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
		if err != nil {
			panic(err)
		}
		consumer.cs = client

		go func() {
			for {
				log.Println("consume at ", consumer.id)
				err := client.Consume(context.Background(), []string{topic}, consumer)
				if err != nil {
					if errors.Is(err, sarama.ErrClosedConsumerGroup) {
						return
					}
					panic(err)
				}
			}
		}()
	}

	<-consumer1.ready // Await till the consumer has been set up
	<-consumer2.ready // Await till the consumer has been set up

	log.Println("Sarama consumers up and running!...")

	for _, t := range []*Consumer{&consumer1, &consumer2} {
		if atomic.LoadInt32(&t.claims) == 0 {
			log.Println("trying to close cs...", t.id)

			if err = t.cs.Close(); err != nil {
				panic(err)
			}

			log.Println("successfully closed cs without partition")

			return
		}
	}

	log.Fatal("test does not work, must not happen")
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	id     int
	ready  chan struct{}
	cs     sarama.ConsumerGroup
	claims int32
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(s sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	log.Println("consumer setup", consumer.id)
	atomic.StoreInt32(&consumer.claims, int32(len(s.Claims())))
	consumer.ready <- struct{}{}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("consumer cleanup", consumer.id)
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}

	return nil
}
