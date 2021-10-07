package meta

import (
	"bufio"
	"database/sql"
	"io"
	"log"
	"os"

	"github.com/pkg/errors"
)

type ID = sql.NullString

type Table struct {
	ID         ID             `json:"id"`
	Type       string         `json:"table-type,omitempty"`
	DatabaseID ID             `json:"database-id"`
	SchemaName sql.NullString `json:"schema-name"`
	Name       string         `json:"table-name"`
	TableStats
	DumpingQuery sql.NullString `json:"dumping-query"`
}

type Column struct {
	ID              ID             `json:"id"`
	TableID         ID             `json:"table-id"`
	Name            string         `json:"column-name"`
	Position        int            `json:"column-position"`
	DataType        sql.NullString `json:"data-type"`
	DataLength      sql.NullInt64  `json:"data-type-length"`
	DataPrecision   sql.NullInt64  `json:"data-type-precision"`
	DataScale       sql.NullInt64  `json:"data-type-scale"`
	RuntimeDataType sql.NullString
	ColumnStats
	ColumnInput
	ExtentColumnID ID            `json:"extent-column-id"`
	ExtentCount    sql.NullInt64 `json:"extent-count"`
	ExtentPosition sql.NullInt64 `json:"extent-position"`
}

type TableStats struct {
	RowCount  sql.NullInt64 `json:"table-row-count"`
	SizeBytes sql.NullInt64 `json:"table-size-bytes"`
}

type ColumnStats struct {
	UniqueCount sql.NullInt64 `json:"column-unique-count"`
	EmptyCount  sql.NullInt64 `json:"column-empty-count"`
}

type ColumnInput struct {
	FusionSeparator string `json:"column-fusion-separator"`
	LeadingChar     string `json:"column-leading-char"`
	StopWords       string `json:"column-stop-words"`
}

var DataIndexStorageFileName = "/iixs.bin"
var DataSparsnessStorageFileName = "/nrcs.bin"

type Columns []*Column

func (cs Columns) ByPosition(i, j int) bool {
	if cs[i].Position == cs[j].Position {
		return cs.ByID(i, j)
	}
	return cs[i].Position < cs[j].Position
}
func (cs Columns) ByID(i, j int) bool {
	return cs[i].ID.String < cs[j].ID.String
}

/*
var columnFileName = "/columns.json"

func WriteToColumnFile(dir string, columns []*Column) (err error) {
	os.MkdirAll(dir, 0777)
	fl, erre := os.Create(dir + columnFileName)
	if erre != nil {
		err = errors.Wrapf(erre, "creating column metadata file in %v", dir)
		return
	}
	defer fl.Close()
	enc := json.NewEncoder(fl)
	err = enc.Encode(columns)
	if err != nil {
		err = errors.Wrapf(err, "encoding column metadata to json file")
		return err
	}
	return
}

func ReadFromColumnFile(dir string) (columns []*Column, err error) {
	fs, err := os.Open(dir + columnFileName)
	if err != nil {
		err = errors.Wrapf(err, "opening column file in %v", dir)
		return
	}
	defer fs.Close()
	cs := make([]*Column, 0, 10)
	dec := json.NewDecoder(fs)
	_, err = dec.Token()
	if err != nil {
		err = errors.Wrapf(err, "Seeking meta.Column json token")
		log.Fatal(err)
	}
	for dec.More() {
		var c Column
		err = dec.Decode(&c)
		if err != nil {
			err = errors.Wrapf(err, "decoding meta.Column json entry")
			return
		}
		cs = append(cs, &c)
	}
	columns = cs
	return
}
*/

/*
var tableFileName = "/table.json"

func WriteToTableFile(dir string, table *Table) (err error) {
	os.MkdirAll(dir, 0777)
	fl, err := os.Create(dir + tableFileName)
	if err != nil {
		err = errors.Wrapf(err, "creating table metadata file in %v", dir)
		return err
	}
	defer fl.Close()
	enc := json.NewEncoder(fl)
	err = enc.Encode(table)
	if err != nil {
		err = errors.Wrapf(err, "encoding table metadata to json file")
		return err
	}
	return
}

func ReadFromTableFile(dir string) (table *Table, err error) {
	fl, err := os.Open(dir + tableFileName)
	if err != nil {
		err = errors.Wrapf(err, "Opening table metadata file in %v", dir)
		return
	}
	defer fl.Close()
	dec := json.NewDecoder(fl)
	table = &Table{}
	err = dec.Decode(table)
	if err != nil {
		err = errors.Wrapf(err, "decoding table metadata from json file")
		return
	}
	return
}*/

func WriteToIndexStorageFile(dir string, fileName string, xs io.WriterTo) (err error) {
	os.MkdirAll(dir, 0777)
	fl, err := os.Create(dir + fileName)
	if err != nil {
		err = errors.Wrapf(err, "creating %v index storage file in %v", fileName, dir)
		return err
	}
	defer fl.Close()
	buf := bufio.NewWriter(fl)
	defer buf.Flush()
	_, err = xs.WriteTo(buf)
	if err != nil {
		err = errors.Wrapf(err, "storing index data to file in %v", fileName, dir)
		return err
	}
	return
}

func ReadFromIndexStorageFile(xs io.ReaderFrom, dir string, fileName string) (err error) {
	fl, err := os.Open(dir + fileName)
	if err != nil {
		err = errors.Wrapf(err, "opening %v index storage file in %v", fileName, dir)
		log.Fatal(err)
	}
	defer fl.Close()
	buf := bufio.NewReader(fl)
	_, err = xs.ReadFrom(buf)
	if err != nil {
		err = errors.Wrapf(err, "restoring index data from file %v in %v", fileName, dir)
		log.Fatal(err)
	}
	return
}
