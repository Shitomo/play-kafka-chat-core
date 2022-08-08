package model

import "github.com/google/uuid"

type SenderId string

func GenerateSenderId() SenderId {
	return SenderId(rune(uuid.New().ID()))
}

func (s SenderId) String() string {
	return string(s)
}
