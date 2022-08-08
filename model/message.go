package model

type Message struct {
	SenderId  SenderId
	Content   string
	CreatedAt Datetime
	UpdatedAt Datetime
}

func NewMessage(senderId string, content string) Message {
	return Message{
		SenderId:  SenderId(senderId),
		Content:   content,
		CreatedAt: Now(),
		UpdatedAt: Now(),
	}
}
