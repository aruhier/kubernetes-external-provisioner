package log

import (
	"github.com/go-logr/logr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func New(v int8) logr.Logger {
	level := zap.NewAtomicLevelAt(zapcore.Level(-1 * v))

	return ctrlzap.New(
		ctrlzap.JSONEncoder(ISO8601TimeEncoder()),
		ctrlzap.UseDevMode(false),
		ctrlzap.Level(level),
	)
}

func ISO8601TimeEncoder() ctrlzap.EncoderConfigOption {
	return func(c *zapcore.EncoderConfig) {
		c.EncodeTime = zapcore.ISO8601TimeEncoder
	}
}
