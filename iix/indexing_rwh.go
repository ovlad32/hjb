package iix

import (
	"context"

	"github.com/ovlad32/hjb/meta"
	"github.com/pkg/errors"
)

type iMetaProvider interface {
	CreateExtentsWithCount(context.Context, *meta.Column, int) ([]*meta.Column, error)
	ColumnById(context.Context, string) (*meta.Column, error)
	Columns(context.Context) ([]*meta.Column, error)
	ColumnIDs(context.Context, []*meta.Column) ([]string, error)
	TotalRowCount(context.Context) (int, error)
}

type IndexingRowHanlder struct {
	meta    iMetaProvider
	norm    iNormalizer
	indexer IIndexer
}

func NewIndexingRowHanlder(mp iMetaProvider, norm iNormalizer, ixr IIndexer) IndexingRowHanlder {
	return IndexingRowHanlder{
		meta:    mp,
		norm:    norm,
		indexer: ixr,
	}
}

func (h IndexingRowHanlder) Handle(ctx context.Context, rowNumber int, originalValues []string) error {
	columns, values, _, err := resolveExtentsFromValues(ctx, h.meta, h.norm, originalValues)
	if err != nil {
		err = errors.WithStack(err)
		return err      
	}
	valueToColumnsMap, err := groupByValue(ctx, columns, values)
	if err != nil {
		err = errors.WithStack(err)
		return err
	}
	{
		for value, columns := range valueToColumnsMap {
			ids, err := h.meta.ColumnIDs(ctx, columns)
			if err != nil {
				err = errors.WithStack(err)
				return err
			}
			err = h.indexer.Index(value, rowNumber, ids)
			if err != nil {
				err = errors.WithStack(err)
				return err
			}
		}
	}
	return nil
}
