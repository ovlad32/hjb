package meta

import (
	"context"
	"fmt"
	"log"

	"github.com/pkg/errors"
)

type iTableMetadataRepository interface {
	FindByID(ctx context.Context, tableID ID) (*Table, error)
	FindByDatabaseAndSchemaAndName(ctx context.Context, dbName, schemaName, tableName string) ([]*Table, error)
	Save(ctx context.Context, table *Table) (*Table, error)
}
type iColumnMetadataRepository interface {
	FindByTableID(ctx context.Context, tableID ID) ([]*Column, error)
	FindMaxExtentPositionByTableId(ctx context.Context, tableID ID) (int, error)
	//MergeColumns(ctx context.Context, tableID ID, cs []*Column) (err error)
	Save(ctx context.Context, column *Column) (*Column, error)
}

type MetadataService struct {
	tableRepo  iTableMetadataRepository
	columnRepo iColumnMetadataRepository
}

func NewMetdataService() MetadataService {
	return MetadataService{}
}

func (s *MetadataService) SetTableMetadataRepository(r iTableMetadataRepository) error {
	s.tableRepo = r
	return nil
}

func (s *MetadataService) SetColumnMetadataRepository(r iColumnMetadataRepository) error {
	s.columnRepo = r
	return nil
}

func (s MetadataService) ComposeExtentName(c *Column, position, count int) string {
	return fmt.Sprintf("%v[%v/%v]", c.Name, position, count)
}

func (s MetadataService) CreateExtentsWithCount(ctx context.Context, c *Column, count int) ([]*Column, error) {
	maxPos, err := s.columnRepo.FindMaxExtentPositionByTableId(ctx, c.TableID)
	if err != nil {
		log.Fatal("s.mdr.FindMaxExtentPositionByTableId(ctx, c.TableID)")
	}

	model := Column{
		TableID:        c.TableID,
		Position:       maxPos,
		ExtentColumnID: c.ID,

		DataType:   c.DataType,
		DataLength: c.DataLength,
	}
	extents := make([]*Column, 0, count)
	for i := 1; i <= count; i++ {
		extent := model
		extent.Position = model.Position + i
		extent.Name = s.ComposeExtentName(c, i, count)
		extentRef, saveErr := s.columnRepo.Save(ctx, &extent)
		if saveErr != nil {
			log.Fatal("s.columnRepo.Save(ctx, extent)")
		}
		extents = append(extents, extentRef)
	}
	return extents, nil
}

func (s MetadataService) FindFirstTableByDatabaseAndSchemaAndName(ctx context.Context, dbName, schemaName, tableName string) (ret *Table, err error) {
	var tables []*Table
	tables, err = s.tableRepo.FindByDatabaseAndSchemaAndName(ctx, dbName, schemaName, tableName)
	if err != nil {
		return
	}
	return tables[0], nil
}

func (s MetadataService) FindColumnsByTableId(ctx context.Context, tableId ID) ([]*Column, error) {
	return s.columnRepo.FindByTableID(ctx, tableId)
}

func (s MetadataService) IsExtent(c *Column) (bool, ID) {
	return c.ExtentColumnID.Valid, c.ExtentColumnID
}

func (s MetadataService) FindTableByID(ctx context.Context, tableID ID) (*Table, error) {
	return s.tableRepo.FindByID(ctx, tableID)
}

func (s MetadataService) MergeColumns(ctx context.Context, tableID ID, columns []*Column) (err error) {
	for i := range columns {
		columns[i].TableID = tableID
		_, err = s.columnRepo.Save(ctx, columns[i])
		if err != nil {
			err = errors.WithStack(err)
			return
		}
	}
	return
	//return s.columnRepo.MergeColumns(ctx, tableID, columns)
}

func (s MetadataService) MergeTable(ctx context.Context, table *Table) (err error) {
	table, err = s.tableRepo.Save(ctx, table)
	return
}
func (s MetadataService) SaveTable(ctx context.Context, table *Table) (saved *Table, err error) {
	return s.tableRepo.Save(ctx, table)
}
func (s MetadataService) SaveColumn(ctx context.Context, column *Column) (saved *Column, err error) {
	return s.columnRepo.Save(ctx, column)
}
