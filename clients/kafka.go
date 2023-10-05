package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"net"
	"time"
)

var kafkaConfig = &sarama.Config{}

func init() {
	kafkaConfig.Producer.Retry.Max = 1
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Metadata.Full = true
	kafkaConfig.Version = sarama.V0_10_0_0
	kafkaConfig.ClientID = fmt.Sprintf("calculator-%s", myIP())
	kafkaConfig.Metadata.Full = true
}

type KafkaProducer struct {
	topic       string
	messageChan chan<- *sarama.ProducerMessage
}

func NewKafkaProducer(addrs []string, topic string) (*KafkaProducer, error) {
	ap, err := sarama.NewAsyncProducer(addrs, kafkaConfig)
	if err != nil {
		return nil, err
	}
	// TODO need to check Successes
	p := &KafkaProducer{
		topic:       topic,
		messageChan: ap.Input(),
	}
	return p, nil
}

func (c *KafkaProducer) Send(ctx context.Context, key string, msg interface{}) error {
	d, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.messageChan <- &sarama.ProducerMessage{
		Topic:     c.topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(d),
		Timestamp: time.Now(),
	}
	return nil
}

type KafkaConsumer struct {
	topic                 string
	messageChan           <-chan *sarama.ConsumerMessage
	latestOffsetRead      int64
	latestOffsetProcessed int64
	partition             int32
	offsetStore           LatestOffsetStore
}

type LatestOffsetStore interface {
	GetLatestConsumeOffset(ctx context.Context, partition int32) (int64, error)
	SetLatestConsumeOffset(ctx context.Context, partition int32, offset int64) error
}

func NewKafkaConsumer(addrs []string, topic string, offsetStore LatestOffsetStore) (*KafkaConsumer, error) {
	consumer, err := sarama.NewConsumer(addrs, kafkaConfig)
	if err != nil {
		return nil, err
	}
	partition := choosePartition()
	// TODO need to adjust partition
	p, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return nil, err
	}
	offset, err := offsetStore.GetLatestConsumeOffset(context.Background(), partition)
	if err != nil {
		return nil, err
	}
	c := &KafkaConsumer{
		topic:                 topic,
		messageChan:           p.Messages(),
		latestOffsetRead:      offset,
		latestOffsetProcessed: offset,
		offsetStore:           offsetStore,
	}
	go func() {
		ticker := time.NewTicker(time.Second * 5)
		for range ticker.C {
			err := c.offsetStore.SetLatestConsumeOffset(context.Background(), c.partition, c.latestOffsetRead)
			if err != nil {
				fmt.Printf("Failed to save consumer offset. partition: %v, offset: %v\n", c.partition, c.latestOffsetRead)
			}
		}
	}()
	return c, nil
}

func choosePartition() int32 {
	// TODO
	return 0
}

func (c *KafkaConsumer) Next() *sarama.ConsumerMessage {
	msg, ok := <-c.messageChan
	if !ok {
		return nil
	}
	c.latestOffsetRead = msg.Offset
	return msg
}

func (c *KafkaConsumer) Commit() error {
	c.latestOffsetProcessed = c.latestOffsetRead
	return nil
}

func myIP() string {
	addrList, err := net.InterfaceAddrs()
	if err != nil {
		return "0.0.0.0"
	}
	for _, addr := range addrList {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.To4().String()
			}
		}
	}
	return "0.0.0.0"
}
