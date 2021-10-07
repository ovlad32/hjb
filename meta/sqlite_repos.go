package meta

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type SqliteTableRepo struct {
	Db *sql.DB
}

type SqliteColumnRepo struct {
	Db *sql.DB
}

type CompositeTableRepo struct {
	DumpDescRepo DumpDescTableRepository
	SqliteRepo   SqliteTableRepo
}

type CompositeColumnRepo struct {
	DumpDescRepo DumpDescColumnRepository
	SqliteRepo   SqliteColumnRepo
}

type sqlExecutor interface {
	ExecContext(c context.Context, sql string, p ...interface{}) (sql.Result, error)
}

func (r *SqliteTableRepo) fieldReferences(table *Table) (refs []interface{}) {
	refs = make([]interface{}, 0, 10)
	refs = append(refs, &table.ID)
	refs = append(refs, &table.Type)
	refs = append(refs, &table.DatabaseID)
	refs = append(refs, &table.Name)
	refs = append(refs, &table.SchemaName)
	refs = append(refs, &table.RowCount)
	refs = append(refs, &table.SizeBytes)
	refs = append(refs, &table.DumpingQuery)
	return
}
func (r SqliteTableRepo) findBy(ctx context.Context, whereExpr string, params []interface{}) (entities []*Table, err error) {
	rs, err := r.Db.QueryContext(ctx,
		`select id, type, mtd_db_id, table_name, schema_name, row_count, size_bytes, dumping_query from mtd_tabs `+whereExpr, params...)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	entities = make([]*Table, 0)
	for rs.Next() {
		entity := &Table{}
		err = rs.Scan(r.fieldReferences(entity)...)
		if err != nil {
			err = errors.WithStack(err)
			break
		}
		entities = append(entities, entity)
	}
	return
}
func (r SqliteTableRepo) FindByID(ctx context.Context, tableID ID) (table *Table, err error) {
	tables, err := r.findBy(ctx, ` where id = ? `, []interface{}{tableID})
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	if len(tables) == 0 {
		err = ItemNotFoundError
		return
	}
	table = tables[0]
	return
}

func (r SqliteTableRepo) FindByDatabaseAndSchemaAndName(ctx context.Context, dbName string, schemaName, tableName string) (tables []*Table, err error) {
	tables, err = r.findBy(ctx, ` where mtd_db_id = ? and schema_name = ? and table_name = ?`,
		[]interface{}{dbName, schemaName, tableName},
	)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	return
}

func (r SqliteTableRepo) Save(ctx context.Context, entity *Table) (saved *Table, err error) {
	var sqlText = "insert into mtd_tabs(id, type, mtd_db_id, table_name, schema_name, row_count, size_bytes, dumping_query) " +
		" values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) " +
		" on conflict (id) do update set row_count=?6, size_bytes=?7, dumping_query=?8 "

	callExec := func(exec sqlExecutor) (err error) {
		rs, err := exec.ExecContext(ctx, sqlText, r.fieldReferences(entity)...)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		if !entity.ID.Valid {
			rs.LastInsertId()
			id, erre := rs.LastInsertId()
			if err != nil {
				err = errors.WithStack(erre)
				return
			}
			entity.ID = ID{
				String: strconv.FormatInt(id, 10),
				Valid:  true,
			}
			return
		}
		return
	}
	tx, ok := ctx.Value("tx").(*sql.Tx)
	if ok && tx != nil {
		err = callExec(tx)
	} else {
		err = callExec(r.Db)
	}
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	saved = entity
	return
}

func (r SqliteColumnRepo) fieldReferences(c *Column) (refs []interface{}) {
	refs = make([]interface{}, 0, 15)
	refs = append(refs, &c.ID)
	refs = append(refs, &c.TableID)
	refs = append(refs, &c.Name)
	refs = append(refs, &c.Position)
	refs = append(refs, &c.DataType)
	refs = append(refs, &c.RuntimeDataType)
	refs = append(refs, &c.EmptyCount)
	refs = append(refs, &c.UniqueCount)
	refs = append(refs, &c.ExtentColumnID)
	refs = append(refs, &c.ExtentPosition)
	refs = append(refs, &c.ExtentCount)
	return
}

func (r SqliteColumnRepo) findBy(ctx context.Context, whereExpr string, params []interface{}) (entities []*Column, err error) {
	sqlText := "select id, mtd_tab_id, column_name, position, data_type, " +
		"runtime_data_type, empty_count, unique_count," +
		"extent_mtd_col_id, extent_position, extent_count " +
		"from mtd_cols "

	rs, err := r.Db.QueryContext(ctx, sqlText+whereExpr, params...)
	if err != nil {
		err = errors.WithStack(err)
		return
	}

	entities = make([]*Column, 0)
	for rs.Next() {
		entity := &Column{}
		err = rs.Scan(r.fieldReferences(entity)...)
		if err != nil {
			err = errors.WithStack(err)
			break
		}
		entities = append(entities, entity)
	}
	return
}

func (r SqliteColumnRepo) FindByTableID(ctx context.Context, tableID ID) (cc []*Column, err error) {
	cc, err = r.findBy(ctx, "where mtd_tab_id=?", []interface{}{tableID})
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	return
}

func (r SqliteColumnRepo) FindMaxExtentPositionByTableId(ctx context.Context, tableID ID) (pos int, err error) {
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

func (r SqliteColumnRepo) Save(ctx context.Context, entity *Column) (saved *Column, err error) {
	var sqlText = "insert into mtd_cols(id, mtd_tab_id, column_name, position, data_type, " +
		"runtime_data_type, empty_count, unique_count," +
		"extent_mtd_col_id, extent_position, extent_count) " +
		" values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) " +
		" on conflict (id) do update set data_type=?5, runtime_data_type=?6, empty_count=?7, unique_count=?8"

	callExec := func(exec sqlExecutor) (err error) {
		rs, err := exec.ExecContext(ctx, sqlText, r.fieldReferences(entity)...)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		if !entity.ID.Valid {
			rs.LastInsertId()
			id, erre := rs.LastInsertId()
			if err != nil {
				err = errors.WithStack(erre)
				return
			}
			entity.ID = ID{strconv.FormatInt(id, 10), true}
			return
		}
		return
	}
	tx, ok := ctx.Value("tx").(*sql.Tx)
	if ok && tx != nil {
		err = callExec(tx)
	} else {
		err = callExec(r.Db)
	}
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	saved = entity
	return
}

/**********************************************************/

func (r CompositeTableRepo) FindByID(ctx context.Context, tableID ID) (table *Table, err error) {
	table, err = r.SqliteRepo.FindByID(ctx, tableID)
	if err != nil {
		if errors.Is(err, ItemNotFoundError) {
			table, err = r.DumpDescRepo.FindByID(ctx, tableID)
			if err != nil {
				err = errors.WithStack(err)
			}
			return
		}
		err = errors.WithStack(err)
	}
	return
}

func (r CompositeTableRepo) FindByDatabaseAndSchemaAndName(ctx context.Context, dbName string, schemaName string, tableName string) (tables []*Table, err error) {
	t, err := r.SqliteRepo.FindByDatabaseAndSchemaAndName(ctx, dbName, schemaName, tableName)
	if err != nil && !errors.Is(err, ItemNotFoundError) {
		err = errors.WithStack(err)
		return
	}
	if len(t) == 0 {
		t, err = r.DumpDescRepo.FindByDatabaseAndSchemaAndName(ctx, dbName, schemaName, tableName)
	}
	if err != nil && !errors.Is(err, ItemNotFoundError) {
		err = errors.WithStack(err)
		return
	}
	tables = t
	return
}

func (r CompositeTableRepo) Save(ctx context.Context, entity *Table) (saved *Table, err error) {
	saved, err = r.SqliteRepo.Save(ctx, entity)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	r.DumpDescRepo.Save(ctx, entity)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	return
}

func (r CompositeColumnRepo) FindByTableID(ctx context.Context, tableID ID) (cc []*Column, err error) {
	var cc1, cc2 []*Column
	cc1, err = r.SqliteRepo.FindByTableID(ctx, tableID)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	
	var tableDesc *TableDescription
	tableDesc, err = r.DumpDescRepo.findTableDescByTableID(tableID)
	if err != nil {
		err = errors.WithStack(err)
		return
	}

	cc2, err = r.DumpDescRepo.FindByTableID(ctx, tableID)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	
	if len(cc1) == 0 {
		for i := range cc2 {
			cc2[i].StopWords = mergeStopWords(",", tableDesc.StopWords, cc2[i].StopWords)
		}
		cc = cc2
		return
	}

	var cc1Map = make(map[string]*Column)
	for i := range cc1 {
		cc1[i].StopWords = mergeStopWords(",", tableDesc.StopWords, cc1[i].StopWords)
		cc1Map[cc1[i].Name] = cc1[i]
	}

	for i := range cc2 {
		c, ok := cc1Map[cc2[i].Name]
		if !ok {
			c, ok = cc1Map[strings.ToLower(cc2[i].Name)]
		}
		if !ok {
			c, ok = cc1Map[strings.ToUpper(cc2[i].Name)]
		}
		if ok {
			c.ColumnInput.FusionSeparator = cc2[i].FusionSeparator
			c.ColumnInput.LeadingChar = cc2[i].LeadingChar
			c.StopWords = mergeStopWords(",", c.StopWords, cc2[i].StopWords)
		}
	}
	cc = cc1

	return
}

func (r CompositeColumnRepo) FindMaxExtentPositionByTableId(ctx context.Context, tableID ID) (pos int, err error) {
	pos, err = r.SqliteRepo.FindMaxExtentPositionByTableId(ctx, tableID)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	if pos == 0 {
		pos, err = r.DumpDescRepo.FindMaxExtentPositionByTableId(ctx, tableID)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
	}
	return
}

func (r CompositeColumnRepo) Save(ctx context.Context, entity *Column) (saved *Column, err error) {
	saved, err = r.SqliteRepo.Save(ctx, entity)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	r.DumpDescRepo.Save(ctx, entity)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	return
}
