package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
)

// Message holds the schema definition for the Message entity.
type Message struct {
	ent.Schema
}

// Fields of the User.
func (Message) Fields() []ent.Field {
	return []ent.Field{
		field.String("sender_id").NotEmpty(),
		field.String("content").NotEmpty(),
		field.Time("created_at"),
		field.Time("updated_at"),
	}
}
