package main

import (
	"bufio"
	"context"
	"database/sql"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/ovlad32/hjb/iix"
	"github.com/ovlad32/hjb/iixs"
	"github.com/ovlad32/hjb/meta"
	"github.com/ovlad32/hjb/sources"
)

func main_indexing(ctx context.Context, mdService meta.MetadataService, tableDesc *meta.TableDescription) (err error) {
	indexingStartTime := time.Now()
	rnsf := iixs.NewRnStorageFactory()

	dataIndexingStorage := iixs.NewMemIndexerStorage(rnsf)
	dataCardinalityStorage := iixs.NewColumnDataCardianlityStorage()
	dataSparsnessStorage := iixs.NewDataSparnessStorage(rnsf)

	dataIndexingStrategy := iix.NewVrcIndexingStrategy(dataIndexingStorage)
	dataCardinalityStrategy := iix.NewDataCardinalityStrategy(dataCardinalityStorage)
	sparsnessRowStrategy := iix.NewDataSparnessStrategy(dataSparsnessStorage)

	ix := iix.NewIndexer()
	_, err = ix.Register(dataCardinalityStrategy)
	_, err = ix.Register(dataIndexingStrategy)
	_, err = ix.Register(sparsnessRowStrategy)

	var indexingHandler iix.IndexingRowHanlder
	var mdProvider *meta.TableMetaCache
	var processedRows = 0

	valService := iix.NewColumnValueService(
		&iix.NormalizeStrategyFactory{},
	)

	if len(tableDesc.DumpFile) > 0 {
		{
			table, err := mdService.FindFirstTableByDatabaseAndSchemaAndName(ctx,
				tableDesc.DatabaseName,
				tableDesc.SchemaName,
				tableDesc.TableName,
			)
			if err != nil {
				err = errors.WithStack(err)
				return err
			}
			mdProvider = meta.NewTableMetaCache(mdService, table.ID)
		}
		indexingHandler = iix.NewIndexingRowHanlder(mdProvider, valService, ix)
		cs, err := mdProvider.Columns(ctx)
		if err != nil {
			err = errors.WithStack(err)
			return err
		}
		err = valService.RegisterAll(cs)
		if err != nil {
			err = errors.WithStack(err)
			return err
		}
		rhAdapter := meta.NewTextRowHandlerAdapter(indexingHandler, tableDesc)
		logger.Info("Start indexing dump file")
		{
			fl, err := os.OpenFile(tableDesc.DumpFile, os.O_RDONLY, 0x444)
			if err != nil {
				err = errors.Wrapf(err, "Opening file %v", tableDesc.DumpFile)
				return err
			}
			defer fl.Close()
			flBuff := bufio.NewReader(fl)
			processedRows, err = sources.TextStream(ctx, flBuff, rhAdapter)
			if err != nil {
				err = errors.WithStack(err)
				return err
			}
		}
	} else if len(tableDesc.DbDescFile) > 0 {
		creds := &meta.DbCredsDesc{}
		logger.Infof("Reading base table datasource configuration...")
		err = creds.Load(tableDesc.DbDescFile)
		if err != nil {
			err = errors.WithStack(err)
			return err
		}
		err = creds.DetermineDumpingColumns(tableDesc)
		dumpingQuery := tableDesc.GetDumpingQuery()
		if err != nil {
			err = errors.WithStack(err)
			return err
		}
		{
			table, err := mdService.FindFirstTableByDatabaseAndSchemaAndName(ctx,
				tableDesc.DatabaseName,
				tableDesc.SchemaName,
				tableDesc.TableName,
			)
			if err != nil {
				err = errors.WithStack(err)
				return err
			}
			mdProvider = meta.NewTableMetaCache(mdService, table.ID)
		}
		indexingHandler = iix.NewIndexingRowHanlder(mdProvider, valService, ix)
		{
			cs, err := mdProvider.Columns(ctx)
			if err != nil {
				err = errors.WithStack(err)
				return err
			}
			err = valService.RegisterAll(cs)
			if err != nil {
				err = errors.WithStack(err)
				return err
			}
		}
		logger.Info("Start indexing. Requesting data from the database...")
		err = creds.RunQuery(
			func(rows *sql.Rows) (err error) {
				logger.Info("Indexing result set")
				processedRows, err = sources.SqlRowsStream(ctx, rows, indexingHandler)
				return err
			},
			dumpingQuery,
		)
		if err != nil {
			err = errors.WithStack(err)
			return err
		}

		table, err := mdProvider.Table(ctx)
		if err != nil {
			err = errors.WithStack(err)
			return err
		}
		table.DumpingQuery = sql.NullString{
			String: dumpingQuery,
			Valid:  true,
		}
	} else {
		err = errors.Errorf("Neither path to dump file nor datasource connection details configured for base table indexing")
		return
	}
	{
		table, erre := mdProvider.Table(ctx)
		if erre != nil {
			err = errors.WithStack(erre)
			return err
		}

		table.RowCount = sql.NullInt64{
			Int64: int64(processedRows),
			Valid: true,
		}
	}
	err = dataCardinalityStorage.Cardinalities(func(columnID string, n uint) (err error) {
		c, erre := mdProvider.ColumnById(ctx, columnID)
		if erre != nil {
			err = errors.WithStack(erre)
			return
		}
		c.UniqueCount = sql.NullInt64{
			Int64: int64(n),
			Valid: true,
		}
		return
	})

	if err != nil {
		err = errors.WithStack(err)
		return err
	}
	err = dataSparsnessStorage.EmptyValuesCounts(func(columnID string, emptyCount uint) (err error) {
		c, erre := mdProvider.ColumnById(ctx, columnID)
		if erre != nil {
			err = errors.WithStack(erre)
			return
		}
		c.EmptyCount = sql.NullInt64{
			Int64: int64(emptyCount),
			Valid: true,
		}
		return
	})
	if err != nil {
		err = errors.WithStack(err)
		return err
	}
	{
		columns, erre := mdProvider.Columns(ctx)
		if erre != nil {
			err = errors.WithStack(erre)
			return
		}
		for i := range columns {
			if !columns[i].EmptyCount.Valid {
				columns[i].EmptyCount = sql.NullInt64{
					Int64: int64(0),
					Valid: true,
				}
			}
			if !columns[i].UniqueCount.Valid {
				columns[i].UniqueCount = sql.NullInt64{
					Int64: int64(0),
					Valid: true,
				}
			}
		}
	}
	err = mdProvider.SaveAll(ctx)
	if err != nil {
		err = errors.WithStack(err)
		return
	}

	logger.Info("Persisting indexing results")
	storageDir, erre := tableDesc.StorageDir()
	if err != nil {
		err = errors.WithStack(erre)
		return err
	}

	err = meta.WriteToIndexStorageFile(storageDir, meta.DataIndexStorageFileName, dataIndexingStorage)
	if err != nil {
		err = errors.Wrapf(err, "Storing %v.%v table index data", tableDesc.SchemaName, tableDesc.TableName)
		return
	}

	err = meta.WriteToIndexStorageFile(storageDir, meta.DataSparsnessStorageFileName, dataSparsnessStorage)
	if err != nil {
		err = errors.Wrapf(err, "Storing %v.%v table empty rows state", tableDesc.SchemaName, tableDesc.TableName)
		return
	}

	/*
		err = meta.WriteToTableFile(storageDir, table)
		if err != nil {
			err = errors.Wrapf(err, "Storing %v.%v table metadata", table.SchemaName, table.Name)
			return
		}

		columns, erre := mdProvider.Columns(ctx)
		if err != nil {
			err = errors.WithStack(erre)
			return
		}

		err = meta.WriteToColumnFile(storageDir, columns)
		if err != nil {
			err = errors.Wrapf(err, "Storing %v.%v table column metadata", table.SchemaName, table.Name)
			return
		}*/

	logger.Infof("Finish indexing. Total time: %v", time.Since(indexingStartTime))
	return
}
