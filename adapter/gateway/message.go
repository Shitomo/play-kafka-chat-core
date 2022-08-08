package gateway

import (
	"context"

	"github.com/Shitomo/my-chat-core/driver/db"
	"github.com/Shitomo/my-chat-core/model"
)

type MessageGateway struct {
	dbClient db.Client
}

func NewMessageGateway(dbClient db.Client) MessageGateway {
	return MessageGateway{
		dbClient: dbClient,
	}
}

func (m MessageGateway) Save(ctx context.Context, message model.Message) error {
	return m.dbClient.Message.Create().
		SetSenderID(message.SenderId.String()).
		SetContent(message.Content).
		SetCreatedAt(message.CreatedAt.Value()).
		SetUpdatedAt(message.UpdatedAt.Value()).
		Exec(ctx)
}
