package sources

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type IRowHandler interface {
	Handle(cx context.Context, rowNumber int, values []string) error
}

type ITextRowHandler interface {
	IRowHandler
	Separator() []byte
}

func TextStream(
	cx context.Context,
	stream io.Reader,
	rh ITextRowHandler,
) (lineNumber int, err error) {
	scanner := bufio.NewScanner(stream)
	scanner.Split(bufio.ScanLines)
	startTime := time.Now()
	tickTime := startTime
	tickLineNumber := 0
	var stringValues []string
	for scanner.Scan() {
		lineNumber++
		if len(scanner.Bytes()) == 0 {
			continue
		}
		byteValues := bytes.Split(scanner.Bytes(), rh.Separator())
		if stringValues == nil {
			stringValues = make([]string, 0, len(byteValues))
		} else {
			stringValues = stringValues[:0]
		}
		for _, b := range byteValues {
			stringValues = append(stringValues, string(b))
		}

		err = rh.Handle(cx, lineNumber, stringValues)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		if time.Since(tickTime).Seconds() >= 1 {
			tickTime = time.Now()
			log.Printf("Processed %v lines. Speed %v lps", lineNumber, lineNumber-tickLineNumber)
			tickLineNumber = lineNumber
		}
	}
	if scanner.Err() != nil {
		err = errors.WithStack(err)
		return
	}
	return
}
