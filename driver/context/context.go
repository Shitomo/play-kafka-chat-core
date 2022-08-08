package context

import (
	"context"
	"math/rand"
	"time"

	"github.com/oklog/ulid"
)

type key string

const (
	ctxReqKey    = key("req")
	ctxStatusKey = key("status")
)

func SetReq(parents context.Context) context.Context {
	t := time.Now()
	/* #nosec */
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	id := ulid.MustNew(ulid.Timestamp(t), entropy)

	return context.WithValue(parents, ctxReqKey, id.String())
}

func GetReqCtx(ctx context.Context) string {
	v := ctx.Value(ctxReqKey)
	id, ok := v.(string)

	if !ok {
		return ""
	}

	return id
}

func SetCode(parents context.Context, status int) context.Context {
	return context.WithValue(parents, ctxStatusKey, status)
}

func GetCode(ctx context.Context) int {
	v := ctx.Value(ctxStatusKey)
	status, ok := v.(int)

	if !ok {
		return 0
	}

	return status
}
