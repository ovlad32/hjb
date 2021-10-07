package meta

import (
	"context"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

type iMetadataService interface {
	FindTableByID(ctx context.Context, tableID ID) (*Table, error)
	CreateExtentsWithCount(context.Context, *Column, int) ([]*Column, error)
	FindColumnsByTableId(ctx context.Context, tableID ID) ([]*Column, error)
	IsExtent(c *Column) (bool, ID)
	SaveTable(ctx context.Context, table *Table) (saved *Table, err error)
	SaveColumn(ctx context.Context, column *Column) (saved *Column, err error)
}

type extentKey struct {
	position int
	count    int
}

func newExtentKey(c *Column, count int) extentKey {
	return extentKey{position: c.Position, count: count}
}

type TableMetaCache struct {
	tableID   ID
	mds       iMetadataService
	initOnce  sync.Once
	columnMap map[string]*Column
	defined   []*Column
	extentMap map[extentKey][]*Column
	table     *Table
}

func NewTableMetaCache(mds iMetadataService, tableID ID) (p *TableMetaCache) {
	p = &TableMetaCache{
		tableID: tableID,
		mds:     mds,
	}
	return
}

func (p *TableMetaCache) init(ctx context.Context) (err error) {
	table, err := p.mds.FindTableByID(ctx, p.tableID)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	p.table = table
	var cs []*Column
	cs, err = p.mds.FindColumnsByTableId(ctx, p.tableID)
	if err != nil {
		err = errors.WithStack(err)
		return
	}

	p.defined = make([]*Column, 0, len(cs))
	p.columnMap = make(map[string]*Column)
	p.extentMap = make(map[extentKey][]*Column)

	for i := range cs {
		p.columnMap[cs[i].ID.String] = cs[i]
		if ok, _ := p.mds.IsExtent(cs[i]); ok {
			extKey := newExtentKey(cs[i], int(cs[i].ExtentCount.Int64))
			var extents []*Column
			if extents, ok = p.extentMap[extKey]; !ok {
				extents = make([]*Column, 0, cs[i].ExtentCount.Int64)
			}
			extents = append(extents, cs[i])
			p.extentMap[extKey] = extents
		} else {
			p.defined = append(p.defined, cs[i])
		}
	}
	sort.Slice(p.defined, func(i, j int) bool {
		return p.defined[i].Position < p.defined[j].Position
	})
	for _, extents := range p.extentMap {
		sort.Slice(extents, func(i, j int) bool {
			return extents[i].Position < extents[j].Position
		})
	}
	return
}

func (p *TableMetaCache) Columns(ctx context.Context) (res []*Column, err error) {
	p.initOnce.Do(func() { err = p.init(ctx) })
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	res = p.defined
	return
}
func (p *TableMetaCache) Table(ctx context.Context) (t *Table, err error) {
	p.initOnce.Do(func() { err = p.init(ctx) })
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	t = p.table
	return
}

func (p *TableMetaCache) CreateExtentsWithCount(ctx context.Context, c *Column, count int) (extents []*Column, err error) {
	var found bool
	p.initOnce.Do(func() { err = p.init(ctx) })
	extKey := newExtentKey(c, count)
	if extents, found = p.extentMap[extKey]; !found {
		extents, err = p.mds.CreateExtentsWithCount(ctx, c, count)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		p.extentMap[extKey] = extents
		for _, extent := range extents {
			p.columnMap[extent.ID.String] = extent
		}
	}
	return
}

func (p TableMetaCache) ColumnIDs(ctx context.Context, columns []*Column) (ids []string, err error) {
	if columns == nil {
		return
	}
	ids = make([]string, 0, len(columns))
	for _, c := range columns {
		if !c.ID.Valid {
			err = errors.Errorf("Column ID has not been initialized %v", c)
			return nil, err
		}
		ids = append(ids, c.ID.String)
	}
	return
}

func (p TableMetaCache) ColumnById(ctx context.Context, id string) (column *Column, err error) {
	var found bool
	p.initOnce.Do(func() { err = p.init(ctx) })
	if column, found = p.columnMap[id]; !found {
		err = errors.Errorf("Couldn't identify column with ID %v", id)
	}
	return
}

func (p TableMetaCache) TotalRowCount(ctx context.Context) (rowCount int, err error) {
	p.initOnce.Do(func() { err = p.init(ctx) })
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	rowCount = int(p.table.RowCount.Int64)
	return
}

func (p TableMetaCache) SaveAll(ctx context.Context) (err error) {
	savedTable, err := p.mds.SaveTable(ctx, p.table)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	for key, c := range p.columnMap {
		c.TableID = savedTable.ID
		savedColumn, erre := p.mds.SaveColumn(ctx, c)
		if err != nil {
			err = errors.WithStack(erre)
			return
		}
		p.columnMap[key] = savedColumn
	}
	return
}
