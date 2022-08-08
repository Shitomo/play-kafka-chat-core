package logger

import (
	"context"
	"fmt"
	"time"

	cx "github.com/Shitomo/my-chat-core/driver/context"
	"github.com/Shitomo/my-chat-core/model"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var log *zap.Logger

const format = "2006-01-02T15:04:05.000Z"

func init() {
	level := zap.InfoLevel

	if model.IsLocalEnv() {
		level = zap.DebugLevel
	}

	encoder := zap.NewDevelopmentEncoderConfig()

	if model.IsLocalEnv() {
		encoder.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	encoder.EncodeTime = zapcore.TimeEncoder(func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.UTC().Format(format))
	})

	zapconf := zap.Config{
		Level:            zap.NewAtomicLevelAt(level),
		Development:      true,
		Encoding:         "console",
		EncoderConfig:    encoder,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}

	log, _ = zapconf.Build()
}

func Infof(ctx context.Context, template string, args ...interface{}) {
	defer func() {
		_ = log.Sync()
	}()
	log.Sugar().Infof(template, args)
}

func Info(ctx context.Context, args ...interface{}) {
	defer func() {
		_ = log.Sync()
	}()
	log.Sugar().Info(args)
}

func Warnf(ctx context.Context, template string, args ...interface{}) {
	l := withCtx(ctx)
	defer func() {
		_ = l.Sync()
	}()
	l.Sugar().Warnf(template, args)
}

func Warn(ctx context.Context, args ...interface{}) {
	l := withCtx(ctx)
	defer func() {
		_ = l.Sync()
	}()
	l.Sugar().Warn(args)
}

func Errorf(ctx context.Context, template string, args ...interface{}) {
	defer func() {
		_ = log.Sync()
	}()
	withCtx(ctx).Sugar().Errorf(template, args)
}

func Error(ctx context.Context, args ...interface{}) {
	l := withCtx(ctx)
	defer func() {
		_ = l.Sync()
	}()
	l.Sugar().Error(args)
}

func Fatalf(ctx context.Context, template string, args ...interface{}) {
	l := withCtx(ctx)
	defer func() {
		_ = l.Sync()
	}()
	l.Sugar().Fatalf(template, args)
}

func Fatal(ctx context.Context, args ...interface{}) {
	l := withCtx(ctx)
	defer func() {
		_ = l.Sync()
	}()
	l.Sugar().Fatal(args)
}

func withCtx(ctx context.Context) *zap.Logger {
	id := cx.GetReqCtx(ctx)

	return log.Named(id)
}

func Access(ctx context.Context, path, method string, start time.Time) {
	elapsed := time.Since(start)
	info := fmt.Sprintf("%s %s %s", path, method, elapsed)
	Info(ctx, info)
}

func Elapsed(ctx context.Context, start time.Time, msg string) {
	elapsed := time.Since(start)
	info := fmt.Sprintf("%s elapsed %s", msg, elapsed)
	Info(ctx, info)
}
