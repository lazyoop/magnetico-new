package utils

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

type ZapLogger struct {
	core     zapcore.Core
	logLevel zapcore.Level // set the log level
	log      *zap.Logger
}

func NewZapLog(logLevel zapcore.Level, isDevEnv bool) *zap.Logger {
	zapLogger := new(ZapLogger)
	zapLogger.logLevel = logLevel
	if isDevEnv {
		zapLogger.MakeDevelopmentLogger()
		return zapLogger.log
	}
	zapLogger.MakeProductionLogger()
	return zapLogger.log
}

// MakeProductionLogger Production mode
func (z *ZapLogger) MakeProductionLogger() {

	//custom zap logger
	loggerLevel := zap.NewAtomicLevel()
	z.core = zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.Lock(os.Stderr),
		loggerLevel)

	loggerLevel.SetLevel(z.logLevel)

	z.log = zap.New(z.core, zap.AddCaller()) //todo: consider removing zap.AddCaller()
}

// MakeDevelopmentLogger Development mode
func (z *ZapLogger) MakeDevelopmentLogger() {

	//custom zap logger
	loggerLevel := zap.NewAtomicLevel()
	z.core = zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.Lock(os.Stderr),
		loggerLevel)

	loggerLevel.SetLevel(z.logLevel)

	z.log = zap.New(z.core, zap.AddCaller())
}
