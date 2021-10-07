package meta

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

var ItemNotFoundError error = errors.New("Not found")
var IDNotValid error = errors.New("Not valid")

type TableDescription struct {
	Tag             string              `json:"tag"`
	DatabaseName    string              `json:"database-name"`
	SchemaName      string              `json:"schema-name"`
	TableName       string              `json:"table-name"`
	DumpFile        string              `json:"dump-file"`
	ColumnSeparator string              `json:"dump-file-column-separator"`
	DbDescFile      string              `json:"db-desc-file"`
	DbQueryFile     string              `json:"db-query-file"`
	StopWords       string              `json:"stop-words"`
	Columns         []ColumnDescription `json:"columns"`
	dumpingQuery    string
	runtime         *Table
}

type ColumnDescription struct {
	Name            string `json:"name"`
	Position        int    `json:"position"`
	DataType        string `json:"data-type"`
	RuntimeDataType string
	FusionSeparator string `json:"fusion-separator,omitempty"`
	LeadingChar     string `json:"leading-char,omitempty"`
	StopWords       string `json:"stop-words,omitempty"`
	runtime         *Column
}

type DbCredsDesc struct {
	Driver                  string `json:"driver"`
	ConnectionString        string `json:"connection-string"`
	ColumnMetaQuery         string `json:"column-meta-query"`
	DbIdentifierQuoteString string `json:"db-identifier-qoute-string"`
	OneRowQueryTemplate     string `json:"one-row-query-template"`
}

/*
   "host":"ec2-3-121-74-61.eu-central-1.compute.amazonaws.com",
   "port":"3306",
   "database":"",
   "user":"memsql",
   "password":"memsql",

*/

type DumpDesc struct {
	tables []TableDescription `json:"table-description"`
}

func (dbCreds *DbCredsDesc) readJson(jsonStream io.Reader) (err error) {
	dec := json.NewDecoder(jsonStream)
	err = dec.Decode(&dbCreds)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	return
}

func (dbCreds *DbCredsDesc) Load(fileName string) (err error) {
	fl, fileErr := os.OpenFile(fileName, os.O_RDONLY, 0x444)
	if fileErr != nil {
		err = errors.Wrapf(fileErr, "Opening file %v", fileName)
		return err
	}
	defer fl.Close()
	err = dbCreds.readJson(fl)
	if err != nil {
		err = errors.Wrapf(err, "Parsing json dbCreds details")
		return
	}
	return
}

func (dbCreds DbCredsDesc) RunQuery(consumerFunc func(r *sql.Rows) error, query string, args ...interface{}) (err error) {
	db, err := sql.Open(dbCreds.Driver, dbCreds.ConnectionString)
	if err != nil {
		err = errors.Wrapf(err, "connecting to the source db")
		return
	}
	defer db.Close()
	rows, err := db.Query(query, args...)
	if err != nil {
		err = errors.Wrapf(err, "running query")
		return
	}
	defer rows.Close()
	err = consumerFunc(rows)
	if err != nil {
		return err
	}
	if err = rows.Err(); err != nil {
		err = errors.Wrapf(err, "iterating over rows")
		return
	}
	return
}

func (dbCreds DbCredsDesc) ToDbIdentifier(item string) (ret string) {
	if len(item) == 0 {
		// TODO: no panic?
		logger.Fatal("item as a db identifier is empty")
	}
	if len(dbCreds.DbIdentifierQuoteString) > 0 {
		ret = fmt.Sprintf("%v%v%v", dbCreds.DbIdentifierQuoteString, item, dbCreds.DbIdentifierQuoteString)
	} else {
		ret = item
	}
	return
}

func (dbCreds DbCredsDesc) collectQueryColumns(query string) (columns []ColumnDescription, err error) {
	columns = make([]ColumnDescription, 0, 10)
	err = dbCreds.RunQuery(func(r *sql.Rows) (err error) {
		r.Next()
		columnNames, err := r.Columns()
		if err != nil {
			err = errors.Wrapf(err, "collecting query column names")
			return
		}
		columnTypes, err := r.ColumnTypes()
		if err != nil {
			err = errors.Wrapf(err, "collecting query column types")
			return
		}
		for i, columnName := range columnNames {
			c := ColumnDescription{}
			c.Name = columnName
			c.Position = i + 1
			c.DataType = columnTypes[i].DatabaseTypeName()
			c.RuntimeDataType = columnTypes[i].ScanType().Name()
			columns = append(columns, c)
		}
		return
	}, query)
	if err != nil {
		err = errors.Wrapf(err, "gathering metadata for query %v", query)
	}
	return
}
func (dbCreds DbCredsDesc) collectDefinedColumns(td *TableDescription) (columns []ColumnDescription, err error) {
	var tableIdentifier = dbCreds.ToDbIdentifier(td.TableName)
	if len(td.SchemaName) > 0 {
		tableIdentifier = dbCreds.ToDbIdentifier(td.SchemaName) + "." + tableIdentifier
	}
	//, strings.Join(queryColumns, ","),
	var query = fmt.Sprintf("select * from %v where 1=0", tableIdentifier)
	columns, err = dbCreds.collectQueryColumns(query)
	if err != nil {
		err = errors.Wrapf(err, "Collect all table's column for %v ", tableIdentifier)
		return
	}
	return
}

func (dc DbCredsDesc) buldDumpingQuery(td *TableDescription) (sqlText string) {
	queryColumns := make([]string, 0, len(td.Columns))
	for _, c := range td.Columns {
		/*switch c.RuntimeDataType {
		case "":
			fallthrough
		case "nil":
			fallthrough
		case "[]byte":
			fallthrough
		case "boolean":
			log.Printf("Skip dumping data from column %v due to its Unknown/raw data type - %v", c.Name, c.RuntimeDataType)
			continue
		}*/
		queryColumns = append(queryColumns, dc.ToDbIdentifier(c.Name))
	}
	tableName := dc.ToDbIdentifier(td.TableName)
	if len(td.SchemaName) > 0 {
		tableName = dc.ToDbIdentifier(td.SchemaName)
	}
	return fmt.Sprintf("select %v from %v", strings.Join(queryColumns, ","), tableName)
}

func (dc DbCredsDesc) DetermineDumpingColumns(td *TableDescription) (err error) {
	var columns []ColumnDescription
	if len(td.DbQueryFile) > 0 {
		td.dumpingQuery, err = td.customDumpingQuery()
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		oneRowQuery := td.dumpingQuery
		if len(dc.OneRowQueryTemplate) > 0 {
			oneRowQuery = fmt.Sprintf(dc.OneRowQueryTemplate, td.dumpingQuery)
		}
		//logger. oneRowQuery
		fmt.Println(oneRowQuery)
		columns, err = dc.collectQueryColumns(oneRowQuery)
		if err != nil {
			err = errors.Wrapf(err, "Collecting column metadata for defined dumping query %v", oneRowQuery)
			return
		}
		fmt.Println(columns)
		td.MergeColumns(columns)
	} else {
		logger.Printf("Collecting %v.%v columns...", td.SchemaName, td.TableName)
		columns, err = dc.collectDefinedColumns(td)
		if err != nil {
			err = errors.Wrapf(err, "Collecting column metadata for %v.%v", td.SchemaName, td.TableName)
			return
		}
		td.MergeColumns(columns)
		td.dumpingQuery = dc.buldDumpingQuery(td)
	}
	return
}

/*

	columns = make([]ColumnDescription, 0, 10)
	err = dbCreds.RunQuery(func(r *sql.Rows) (err error) {
		columnNames, err := r.Columns()
		if err != nil {
			err = errors.Wrapf(err, "collecting query column names")
			return
		}
		columnTypes, err := r.ColumnTypes()
		if err != nil {
			err = errors.Wrapf(err, "collecting query column types")
			return
		}
		for i, columnName := range columnNames {
			c := ColumnDescription{}
			c.Name = columnName
			c.Position = i + 1
			c.DataType = columnTypes[i].DatabaseTypeName()
			c.RuntimeDataType = columnTypes[i].ScanType().Name()
			columns = append(columns, c)
		}
		return
	}, dbCreds.ColumnMetaQuery)
	if err != nil {
		err = errors.Wrapf(err, "gathering metadata for query %v", query)
	}
	return
*/

func (dd *DumpDesc) Load(metaFile string) (err error) {
	f, err := os.Open(metaFile)
	if err != nil {
		err = errors.Wrapf(err, "couldn't open dump description file: %v", metaFile)
		return err
	}
	defer f.Close()

	dd.tables = make([]TableDescription, 0, 10)
	dec := json.NewDecoder(f)
	// read opening bracket
	_, err = dec.Token()
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	for dec.More() {
		var table TableDescription
		err = dec.Decode(&table)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		if len(table.StopWords) > 0 {
			for i := range table.Columns {
				if len(table.Columns[i].StopWords) == 0 {
					table.Columns[i].StopWords = table.StopWords
					continue
				}
				table.Columns[i].StopWords = strings.Join([]string{table.Columns[i].StopWords, table.StopWords}, ",")
			}
		}
		dd.tables = append(dd.tables, table)
	}

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	return
}

func (dd *DumpDesc) Table(db, schema, name string) (ret *TableDescription, found bool) {
	for i := range dd.tables {
		if dd.tables[i].DatabaseName == db && dd.tables[i].SchemaName == schema && dd.tables[i].TableName == name {
			return &dd.tables[i], true
		}
	}
	return
}
func (dd *DumpDesc) TaggedTable(tag string) (ret *TableDescription, found bool) {
	for i := range dd.tables {
		if dd.tables[i].Tag == tag {
			return &dd.tables[i], true
		}
	}
	return
}

func mergeStopWords(sep string, stopWords ...string) string {
	var sm = make(map[string]bool)
	for _, sw := range stopWords {
		for _, w := range strings.Split(sw, sep) {
			sw = strings.TrimSpace(sw)
			if len(sw) > 0 {
				sm[w] = false
			}
		}
	}
	var ss = make([]string, 0, len(sm))
	for w, _ := range sm {
		ss = append(ss, w)
	}
	return strings.Join(ss, sep)
}

func (td *TableDescription) MergeColumns(columns []ColumnDescription) {
outer:
	for i := range td.Columns {
		for j := range columns {
			if strings.ToLower(td.Columns[i].Name) == strings.ToLower(columns[j].Name) {
				columns[j].LeadingChar = td.Columns[i].LeadingChar
				columns[j].FusionSeparator = td.Columns[i].FusionSeparator
				columns[j].StopWords = mergeStopWords(",", columns[j].StopWords, td.Columns[i].StopWords)
				continue outer
			}
		}
		td.Columns[i].Position = len(columns) + 1
		columns = append(columns, td.Columns[i])
	}
	td.Columns = columns
	sort.Slice(td.Columns, func(i, j int) bool {
		return td.Columns[i].Position < td.Columns[j].Position
	})
}

func (td TableDescription) customDumpingQuery() (text string, err error) {
	b, fileErr := ioutil.ReadFile(td.DbQueryFile)
	if fileErr != nil {
		err = errors.Wrapf(fileErr, "Reading SQL statement from %v", td.DbQueryFile)
		return
	}
	text = string(b)
	return
}
func (td TableDescription) GetDumpingQuery() string {
	return td.dumpingQuery
}

func (dd *DumpDesc) findTableDescByTableID(tableID ID, refreshFunc func()) (tdRef *TableDescription, err error) {
	if !tableID.Valid {
		err = errors.Wrap(IDNotValid, "TableID is not valid")
		return
	}
	for _, first := range [2]bool{true, false} {
		for i := range dd.tables {
			td := dd.tables[i]
			if found := td.runtime != nil && td.runtime.ID.String == tableID.String; found {
				tdRef = &td
				return
			}
		}
		if refreshFunc == nil {
			break
		} else if first {
			refreshFunc()
		}
	}
	err = errors.Wrapf(ItemNotFoundError, "No columns found for table ID %v", tableID)
	return
}

func (td TableDescription) StorageDir() (dir string, err error) {
	return fmt.Sprintf("./fs/%s.%s", td.SchemaName, td.TableName), nil
}
