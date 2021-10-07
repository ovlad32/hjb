package meta

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

/*
type IService interface {
	Separator(tableId string) string
}

type IColumnService interface {
	Column(id string) (meta.Column, error)
	FusionColumnName(name string, pos int, total int) string
}*/

type DumpDescTableRepository struct {
	dd        *DumpDesc
	refs      map[ID]*TableDescription
	refsOnce  sync.Once
	refsMutex sync.Mutex
	//tables      []*Table
}
type columnDescRefence struct {
	tableDesc   *TableDescription
	columnIndex int
}
type DumpDescColumnRepository struct {
	dd        *DumpDesc
	refs      map[ID]columnDescRefence
	refsOnce  sync.Once
	refsMutex sync.Mutex
	/*convertOnce sync.Once
	columns     map[string][]*Column*/
}

func NewDumpDescRepos(d *DumpDesc) (DumpDescTableRepository, DumpDescColumnRepository) {
	return DumpDescTableRepository{dd: d},
		DumpDescColumnRepository{dd: d}
}

var hf = fnv.New64()
var hfMx sync.Mutex

func MakeID(s ...string) ID {
	return makeID(s...)
}
func makeID(s ...string) ID {
	ss := strings.Join(s, ".")
	/*hfMx.Lock()
	hf.Reset()
	hf.Write([]byte(ss))
	hash := hf.Sum64()
	hashValue := strconv.FormatUint(hash, 36)
	hfMx.Unlock()*/
	hashValue := ss
	return ID{hashValue, true}
}

/*
func ColumnNamesFromIDs(metaSide string, ids []string) (names []string, err error) {
	names = make([]string, 0, len(ids))
	for _, id := range ids {
		parts := strings.SplitN(id, ".", -1)
		name := parts[len(parts)-1]
		if name == "0-0" {
			name = parts[len(parts)-2]
		} else {
			name = parts[len(parts)-2] + "." + name
		}
		names = append(names, name)
	}
	return
}
*/
func makeColumnID(c *Column) ID {
	if c.ExtentColumnID.Valid {
		return makeID(c.TableID.String, c.Name, fmt.Sprintf("%v-%v", c.ExtentPosition.Int64, c.ExtentCount.Int64))
	}

	return makeID(c.TableID.String, c.Name)
}

func makeTableID(t *Table) ID {
	if t.SchemaName.Valid {
		return makeID(t.DatabaseID.String, t.SchemaName.String, t.Name)
	} else {
		return makeID(t.DatabaseID.String, t.Name)
	}
}

func makeDatabaseID(db string) ID {
	return makeID([]string{db}...)
}

/*
func convertTables(dd *DumpDesc) []*Table {
	tt := make([]*Table, 0, 2)
	for _, td := range dd.tables {
		t := &Table{
			DatabaseID: makeDatabaseID(td.DatabaseName),
			SchemaName: td.SchemaName,
			Name:       td.TableName,
		}
		t.ID = makeTableID(t)
		tt = append(tt, t)
	}
	return tt
}*/
func convertTable(td *TableDescription) (t *Table) {
	t = &Table{
		DatabaseID: makeDatabaseID(td.DatabaseName),
		Name:       td.TableName,
	}
	if len(td.SchemaName) > 0 {
		t.SchemaName = sql.NullString{
			String: td.SchemaName,
			Valid:  true,
		}
	}

	t.ID = makeTableID(t)
	return t
}

func (r *DumpDescTableRepository) initRefs() {
	r.refs = make(map[ID]*TableDescription)
}
func (r *DumpDescTableRepository) refreshTableReferences() {
	r.refsMutex.Lock()
	for i := range r.dd.tables {
		if rt := r.dd.tables[i].runtime; rt != nil {
			if _, found := r.refs[rt.ID]; !found {
				r.refs[rt.ID] = &r.dd.tables[i]
			}
		} else {
			rt = convertTable(&r.dd.tables[i])
			r.dd.tables[i].runtime = rt
			r.refs[rt.ID] = &r.dd.tables[i]
		}
	}
	r.refsMutex.Unlock()
}

func (r *DumpDescTableRepository) FindByID(ctx context.Context, tableID ID) (table *Table, err error) {
	r.refsOnce.Do(r.initRefs)
	td, err := r.dd.findTableDescByTableID(tableID, r.refreshTableReferences)
	if err != nil {
		err = errors.WithStack(err)
	}
	table = td.runtime
	return
}

func (r *DumpDescTableRepository) FindByDatabaseAndSchemaAndName(ctx context.Context, dbName string, schemaName, tableName string) (ret []*Table, err error) {
	r.refsOnce.Do(r.initRefs)
	r.refreshTableReferences()
	tables := make([]*Table, 0, 1)
	for i := range r.dd.tables {
		if r.dd.tables[i].DatabaseName == dbName && r.dd.tables[i].SchemaName == schemaName && r.dd.tables[i].TableName == tableName {
			tables = append(tables, r.dd.tables[i].runtime)
		}
	}
	return tables, nil
}

func (r *DumpDescTableRepository) Save(ctx context.Context, table *Table) (ret *Table, err error) {
	r.refsOnce.Do(r.initRefs)
	r.refreshTableReferences()
	if ref, found := r.refs[table.ID]; table.ID.Valid && found {
		ref.runtime = table
		logger.Printf("Table %v updated", table)
		ret = table
	} else {
		logger.Fatalf("Couldn't find table %v ", table)
	}
	return
}

/*func convertColumns(dd *DumpDesc) map[string][]*Column {
	cm := make(map[string][]*Column)
	for _, td := range dd.tables {
		databaseID := makeDatabaseID(td.DatabaseName)
		tableID := makeID(databaseID.String, td.SchemaName, td.TableName)
		cc := make([]*Column, 0, 10)
		for i, cd := range td.Columns {
			c := &Column{
				TableID:        tableID,
				Name:           cd.Name,
				Position:       i + 1,
				DataType:       sql.NullString{String: cd.DataType, Valid: true},
				DataLength:     sql.NullInt64{},
				DataPrecision:  sql.NullInt64{},
				DataScale:      sql.NullInt64{},
				ColumnStats:    ColumnStats{},
				ColumnInput:    ColumnInput{},
				ExtentColumnID: ID{},
				ExtentCount:    0,
				ExtentPosition: 0,
			}
			c.ID = makeColumnID(c)
			cc = append(cc, c)
		}
		cm[tableID.String] = cc
	}
	return cm
}*/

func convertColumn(td *TableDescription, index int) (c *Column) {
	databaseID := makeDatabaseID(td.DatabaseName)
	tableID := makeID(databaseID.String, td.SchemaName, td.TableName)
	c = &Column{
		TableID:  tableID,
		Name:     td.Columns[index].Name,
		Position: index + 1,
		DataType: sql.NullString{
			String: td.Columns[index].DataType,
			Valid:  true,
		},
		DataLength:    sql.NullInt64{},
		DataPrecision: sql.NullInt64{},
		DataScale:     sql.NullInt64{},
		RuntimeDataType: sql.NullString{
			String: td.Columns[index].RuntimeDataType,
			Valid:  true,
		},
		ColumnStats: ColumnStats{},
		ColumnInput: ColumnInput{
			FusionSeparator: td.Columns[index].FusionSeparator,
			LeadingChar:     td.Columns[index].LeadingChar,
			StopWords:       td.Columns[index].StopWords,
		},
		ExtentColumnID: ID{},
		ExtentCount:    sql.NullInt64{},
		ExtentPosition: sql.NullInt64{},
	}
	c.ID = makeColumnID(c)
	return
}
func (r *DumpDescColumnRepository) initRefs() {
	r.refs = make(map[ID]columnDescRefence)
}
func (r *DumpDescColumnRepository) refreshTableReferences() {
	r.refsMutex.Lock()
	for i := range r.dd.tables {
		if trt := r.dd.tables[i].runtime; trt == nil {
			r.dd.tables[i].runtime = convertTable(&r.dd.tables[i])
		}
	}
	r.refsMutex.Unlock()
}
func (r *DumpDescColumnRepository) refreshColumnReferences() {
	r.refsMutex.Lock()
	for i := range r.dd.tables {
		for j := range r.dd.tables[i].Columns {
			if crt := r.dd.tables[i].Columns[j].runtime; crt != nil {
				if _, found := r.refs[crt.ID]; !found {
					r.refs[crt.ID] = columnDescRefence{
						tableDesc:   &r.dd.tables[i],
						columnIndex: j,
					}
				}
			} else {
				crt = convertColumn(&r.dd.tables[i], j)
				r.dd.tables[i].Columns[j].runtime = crt
				r.refs[crt.ID] = columnDescRefence{
					tableDesc:   &r.dd.tables[i],
					columnIndex: j,
				}
			}
		}
	}
	r.refsMutex.Unlock()
}
func (r *DumpDescColumnRepository) findTableDescByTableID(tableID ID) (tdRef *TableDescription, err error) {
	r.refsOnce.Do(r.initRefs)
	tdRef, err = r.dd.findTableDescByTableID(tableID, r.refreshTableReferences)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	r.refreshColumnReferences()
	return
}

func (r *DumpDescColumnRepository) FindByTableID(ctx context.Context, tableID ID) (cc []*Column, err error) {
	r.refsOnce.Do(r.initRefs)
	td, err := r.findTableDescByTableID(tableID)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	cc = make([]*Column, 0, len(td.Columns))
	for _, cd := range td.Columns {
		if cd.runtime != nil {
			cc = append(cc, cd.runtime)
		}
	}
	return
}

/*
func (r *DumpDescColumnRepository) MergeColumns(ctx context.Context, tableID ID, cs []*Column) (err error) {
	td, err := r.findTableDescByTableID(tableID)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	csIDToPosition := make(map[string]int)
	ctPositionToID := make(map[int]string)

	for i := range td.Columns {
		ctPositionToID[td.Columns[i].Position] = cs[i].ID.String
	}
	for j := range cs {
		cs[j].TableID = tableID
		csIDToPosition[cs[j].ID.String] = cs[j].Position
	}

outer:
	for j := range cs {
		_, err = r.Save(ctx, cs[j])
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		for i := range td.Columns {
			if cs[j].Name == td.Columns[i].Name &&
				cs[j].ExtentCount == td.Columns[i].runtime.ExtentCount &&
				cs[j].ExtentPosition == td.Columns[i].runtime.ExtentPosition {
				if cs[j].ExtentColumnID.String != "" {
					if parentColumnPosition, found := csIDToPosition[cs[j].ExtentColumnID.String]; found {
						if parentID, found := ctPositionToID[parentColumnPosition]; found {
							cs[j].ExtentColumnID.String = parentID
						} else {
							log.Printf("could not find parent column ID for column %v with extents of %v and extent position %v",
								cs[j].Name, cs[j].ExtentCount, cs[j].ExtentPosition)
							continue
						}
					} else {
						log.Printf("could not find parent column position for column %v with extents of %v and extent position %v",
							cs[j].Name, cs[j].ExtentCount, cs[j].ExtentPosition)
						continue
					}
				}
				continue outer
			}
		}
	}
	return
}*/

func (r *DumpDescColumnRepository) Save(ctx context.Context, column *Column) (ret *Column, err error) {
	r.refsOnce.Do(r.initRefs)
	ref, found := r.refs[column.ID]
	if column.ID.Valid && found {
		logger.Printf("Column ID %v updated", column.ID)
		ref.tableDesc.Columns[ref.columnIndex].runtime = column
		ret = column
		return
	}
	td, erre := r.findTableDescByTableID(column.TableID)
	if erre != nil {
		err = errors.WithStack(erre)
	}
	if column.Position <= 0 {
		column.Position, err = r.FindMaxExtentPositionByTableId(ctx, column.TableID)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		column.Position++
	}

	td.Columns = append(td.Columns, ColumnDescription{
		Position:        column.Position,
		Name:            column.Name,
		DataType:        column.DataType.String,
		FusionSeparator: column.FusionSeparator,
		LeadingChar:     column.LeadingChar,
		StopWords:       column.StopWords,
		runtime:         column,
	})
	if !column.ID.Valid {
		column.ID = makeColumnID(column)
	}
	logger.Printf("Column ID %v added", column.ID)
	r.refs[column.ID] = columnDescRefence{
		tableDesc:   td,
		columnIndex: len(td.Columns) - 1,
	}
	ret = column
	return
}

func (r DumpDescColumnRepository) FindMaxExtentPositionByTableId(ctx context.Context, tableID ID) (pos int, err error) {
	cc, err := r.FindByTableID(ctx, tableID)
	if err != nil {
		errors.Wrapf(err, "Searching max column position for table ID %v", tableID)
		return
	}
	for _, c := range cc {
		if c.Position > pos {
			pos = c.Position
		}
	}
	return
}

/*

func (t tbs) ColumnName(index int) string {
	return t.dd.Columns[index].ColumnName
}

func (t tbs) FusionColumnName(name string, index int, total int) string {
	if index == 0 && total == 1 {
		return name
	} else {
		return name + fmt.Sprintf("[%/%v]", index+1, total)
	}
}

func (t tbs) Split(index int, value string) []string {
	sep := t.dd.Columns[index].FusionSep
	if sep == "" {
		return []string{value}
	} else {
		return strings.Split(value, sep)
	}
}

func (t tbs) Reduce(index int, value string) string {
	lc := t.dd.Columns[index].LeadingChar
	if lc == "" {
		return value
	} else {
		return strings.TrimLeft(value,lc)
	}
}


func (t tbs) Separator() string {
	return t.dd.ColumnSep
}
*/
