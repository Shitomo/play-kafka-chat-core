package gateway

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/Shitomo/play-kafka-chat-core/model"

	"github.com/Shopify/sarama"
)

const RealtimeMessageTopic = "realtime-message-topic"

type RealtimeMessagePublisher struct {
	asyncProducer sarama.AsyncProducer
}

func NewRealtimeMessagePublisher(producer sarama.AsyncProducer) RealtimeMessagePublisher {
	return RealtimeMessagePublisher{
		asyncProducer: producer,
	}
}

// 送信メッセージ
type SendMessage struct {
	SenderId  string `json:"senderId"`
	Content   string `json:"content"`
	CreatedAt int64  `json:"createdAt"`
	UpdatedAt int64  `json:"updatedAt"`
}

func (u RealtimeMessagePublisher) Produce(ctx context.Context, message model.Message) error {

	timestamp := time.Now().UnixNano()

	send := &SendMessage{
		SenderId:  message.SenderId.String(),
		Content:   message.Content,
		CreatedAt: int64(message.CreatedAt.UnixMilli()),
		UpdatedAt: int64(message.UpdatedAt.UnixMilli()),
	}

	jsBytes, err := json.Marshal(send)
	if err != nil {
		panic(err)
	}

	msg := &sarama.ProducerMessage{
		Topic: RealtimeMessageTopic,
		Key:   sarama.StringEncoder(strconv.FormatInt(timestamp, 10)),
		Value: sarama.StringEncoder(string(jsBytes)),
	}

	u.asyncProducer.Input() <- msg

	select {
	case <-u.asyncProducer.Successes():
		return nil
	case err := <-u.asyncProducer.Errors():
		return err
	case <-ctx.Done():
		return nil
	}
}

type RealtimeMessageConsumer struct {
	consumer sarama.PartitionConsumer
}

func NewRealtimeMessageConsumer(consumer sarama.PartitionConsumer) RealtimeMessageConsumer {
	return RealtimeMessageConsumer{
		consumer: consumer,
	}
}

func (u RealtimeMessageConsumer) Consume(ctx context.Context) (chan model.Message, chan error) {
	ch := make(chan model.Message, 1)
	errCh := make(chan error, 1)
	// コンシューマールーチン
	go func(ch chan model.Message, errCh chan error) {
	CONSUMER_FOR:
		for {
			select {
			case msg := <-u.consumer.Messages():
				var message SendMessage
				if err := json.Unmarshal(msg.Value, &message); err != nil {
					errCh <- err
					continue
				}
				ch <- model.Message{
					SenderId:  model.SenderId(message.SenderId),
					Content:   message.Content,
					CreatedAt: model.NewDatetimeFromUnixMilli(message.CreatedAt),
					UpdatedAt: model.NewDatetimeFromUnixMilli(message.UpdatedAt),
				}
			case err := <-u.consumer.Errors():
				errCh <- err
			case <-ctx.Done():
				close(ch)
				close(errCh)
				break CONSUMER_FOR
			}
		}
	}(ch, errCh)
	return ch, errCh
}
