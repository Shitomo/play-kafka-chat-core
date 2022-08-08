package gateway

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/Shitomo/my-chat-core/model"

	"github.com/Shopify/sarama"
)

const RealtimeMessageTopic = "realtime-message-topic"

type RealtimeMessageGateway struct {
	asyncProducer sarama.AsyncProducer
}

func NewRealtimeMessageGateway(producer sarama.AsyncProducer) RealtimeMessageGateway {
	return RealtimeMessageGateway{
		asyncProducer: producer,
	}
}

// 送信メッセージ
type SendMessage struct {
	SenderId  string `json:"senderId"`
	Content   string `json:"content"`
	CreatedAt uint64 `json:"createdAt"`
	UpdatedAt uint64 `json:"updatedAt"`
}

func (u RealtimeMessageGateway) Produce(ctx context.Context, message model.Message) error {

	timestamp := time.Now().UnixNano()

	send := &SendMessage{
		SenderId:  message.SenderId.String(),
		Content:   message.Content,
		CreatedAt: uint64(message.CreatedAt.UnixMilli()),
		UpdatedAt: uint64(message.UpdatedAt.UnixMilli()),
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
