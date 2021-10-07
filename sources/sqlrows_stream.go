package sources

import (
	"context"
	"database/sql"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/pkg/errors"
)

func SqlRowsStream(
	cx context.Context,
	stream *sql.Rows,
	rh IRowHandler,
) (rowCount int, err error) {
	var valueRefs []interface{}
	var sqlValues []sql.NullString
	var stringValues []string
	startTime := time.Now()
	tickTime := startTime
	tickRowNumber := 0
	for stream.Next() {
		rowCount++
		if valueRefs == nil {
			columns, erre := stream.Columns()
			if erre != nil {
				err = errors.Wrapf(erre, "Reading number of columns")
				return
			}
			valueRefs = make([]interface{}, len(columns))
			sqlValues = make([]sql.NullString, len(valueRefs))
			stringValues = make([]string, len(valueRefs))
			for i := range valueRefs {
				valueRefs[i] = &sqlValues[i]
			}
		}
		err = stream.Scan(valueRefs...)
		if err != nil {
			err = errors.Wrapf(err, "scanning row #%v", rowCount)
			return
		}
		for i := range sqlValues {
			if !sqlValues[i].Valid {
				stringValues[i] = ""
			} else {
				stringValues[i] = sqlValues[i].String
			}
		}
		err = rh.Handle(cx, rowCount, stringValues)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		if time.Since(tickTime).Seconds() >= 1 {
			tickTime = time.Now()
			log.Printf("Processed %v rows. Speed %v rps", rowCount, rowCount-tickRowNumber)
			tickRowNumber = rowCount
		}
	}

	if stream.Err() != nil {
		err = errors.WithStack(err)
		return
	}
	return
}
