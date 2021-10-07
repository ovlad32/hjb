package meta

import (
	"context"

	log "github.com/sirupsen/logrus"
)

var logger *log.Logger

func SetLogger(l *log.Logger) {
	logger = l
}

type iRowHandler interface {
	Handle(ctx context.Context, rowNumber int, values []string) error
}
