package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"math"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/ovlad32/hjb/iix"
	"github.com/ovlad32/hjb/iixs"
	"github.com/ovlad32/hjb/meta"
	"github.com/ovlad32/hjb/sources"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

/*
github.com/mattn/go-sqlite3

go test -memprofile mem.out
go tool pprof perftest00.test mem.out

go test -cpuprofile cpu.out
go tool pprof perftest00.test cpu.out

rm hjb.db3; go run . -mode=init

go run . -mode=indexing -baseTag="mt-base"
go run . -mode=discovery -baseTag="mt-base" -lakeTag="cra-lake" -anchorUniqueness=1 -anchorMinCount=3
go run . -mode=aggregation -baseTag="mt-base" -baseRowMinPct=5




rm hjb.db3; go run . -mode=init;go run . -mode=discovery -baseTag="mt-base" -lakeTag="cra-lake" -minRowCount -leadingUniqueness=0;
#go run . -mode=aggregation -baseTag="mt-base" -minJoints=1  -minRows=10 -maxCardRange=0 -mergeBlanks=true >1
go run . -mode=aggregation -baseTag="mt-base" -minJointCount=1  -minRowCount=10


rm hjb.db3; go run . -mode=init \
go run . -mode=indexing -baseTag="mt-base" \
go run . -mode=discovery -baseTag="mt-base" -lakeTag="cra-lake" -leadingUniqueness=0\
go run . -mode=aggregation -baseTag="mt-base" -minJointCount=1  -minRowCount=0


go run . -mode=indexing -baseTag="grp1-base"
go run . -mode=discovery -baseTag="grp1-base" -lakeTag="grp1-lake" -leadingUniqueness=3
go run . -mode=aggregation -baseTag="grp1-base" -minJointCount=1  -minRowCount=10
*/
var logger = log.New()

//var sqlDbFile string = "./hjb.db3"
//var sqlDbFile string = "file:./hjb.db3?_journal_mode=OFF&_locking_mode=NORMAL&_txlock=deferred&cache=private&_sync=OFF"
//var sqlDbFile string = "file:./hjb.db3?_sync=OFF&_journal_mode=MEMORY&_locking_mode=NORMAL&cache=shared&_vacuum=none"

var mode string
var metaFile string
var workConfFile string
var baseTableTag string
var lakeTableTag string

//var discoveryFile string
var anchorMinCount int
var anchorMaxCount int
var baseRowMinPct float64
var anchorUniqueness float64

//var mergeBlanks bool

func init() {
	flag.StringVar(&mode, "mode", "", "usage mode: indexing,discovery,aggregation")
	flag.StringVar(&workConfFile, "conf", "./workconf.mtdsc.json", "config json file")
	flag.StringVar(&metaFile, "meta", "./tables.mtdsc.json", "Metadata json file")
	flag.StringVar(&baseTableTag, "baseTag", "", "Tag for base table (indexed side)")
	flag.StringVar(&lakeTableTag, "lakeTag", "", "Tag for lake table (discovered side)")
	//flag.StringVar(&discoveryFile, "discovery", "./discovery.bin", "Discovery file name")
	flag.Float64Var(&anchorUniqueness, "anchorUniqueness", 10, "Uniqueness of a base column to be an anchor, % ")
	flag.IntVar(&anchorMinCount, "anchorMinCount", 1, "Min number of anchor columns in base table in a discovered column group")
	flag.IntVar(&anchorMaxCount, "anchorMaxCount", math.MaxInt8, "Max number of anchor columns in base table in a discovered column group")
	flag.Float64Var(&baseRowMinPct, "baseRowMinPct", 10, "Minimal percentage of the base table rows covered by a group, %")
	//flag.Float64Var(&maxCardRange, "maxCardRange", 0, "Pair column cardinality range max")
	//flag.BoolVar(&mergeBlanks, "mergeBlanks", false, "mergeBlanks")

	flag.Parse()
	logger.Out = os.Stdout

	iix.SetLogger(logger)
	meta.SetLogger(logger)

}

type WorkConfType struct {
	SqliteConnString         string `json:"sqlite-conn-string"`
	ShowCallerInLog          bool   `json:"show-caller-in-log"`
	AggMergeThreadCount      int    `json:"aggregation-merge-thread-count"`
	AggPersistingThreadCount int    `json:"aggregation-persisting-thread-count"`
	AggPersistingBatchSise   int    `json:"aggregation-persisting-batch-size"`
	AggBsCacheSise           int    `json:"aggregation-bs-cache-size"`
}

var WorkConf WorkConfType

func readWorkConf() (wc WorkConfType, err error) {
	fl, err := os.OpenFile(workConfFile, os.O_RDONLY, 0x444)
	if err != nil {
		err = errors.Wrapf(err, "Opening file %v", workConfFile)
		return
	}
	defer fl.Close()
	dec := json.NewDecoder(fl)
	err = dec.Decode(&wc)
	if err != nil {
		err = errors.Wrapf(err, "Parsing json work config")
	}
	return
}

func main() {

	logger.SetLevel(log.DebugLevel)
	logger.SetReportCaller(true)
	logger.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})

	var err error
	ctx := context.TODO()
	wc, err := readWorkConf()
	if err != nil {
		err = errors.Wrap(err, "couldn't load work conf file")
		log.Fatal(err)
	}
	logger.SetReportCaller(wc.ShowCallerInLog)
	var dumpDesc = meta.DumpDesc{}

	err = dumpDesc.Load(metaFile)
	if err != nil {
		err = errors.Wrap(err, "couldn't load dump description file")
		log.Fatal(err)
	}

	if wc.SqliteConnString == "" {
		wc.SqliteConnString = "file:./hjb.db3"
	}

	db, err := sql.Open("sqlite3", wc.SqliteConnString)
	if err != nil {
		logger.Fatal(err)
	}
	defer db.Close()

	mdService := meta.NewMetdataService()
	{
		dumpDescTableRepo, dumpDescColumnRepo := meta.NewDumpDescRepos(&dumpDesc)
		sqliteTableRepo := meta.SqliteTableRepo{
			Db: db,
		}

		sqliteColumnRepo := meta.SqliteColumnRepo{
			Db: db,
		}

		mdService.SetTableMetadataRepository(&meta.CompositeTableRepo{
			DumpDescRepo: dumpDescTableRepo,
			SqliteRepo:   sqliteTableRepo,
		})
		mdService.SetColumnMetadataRepository(&meta.CompositeColumnRepo{
			DumpDescRepo: dumpDescColumnRepo,
			SqliteRepo:   sqliteColumnRepo,
		})
	}

	switch strings.ToLower(mode) {
	case "init":
		sqliteInit(wc)
	case "indexing":
		{
			if baseTableTag == "" {
				log.Fatal("Base table tag has not been specified")
			}
			baseTableDescr, found := dumpDesc.TaggedTable(baseTableTag)
			if !found {
				log.Fatalf("Table with %v tag has not been found", baseTableTag)
			}
			indexingErr := main_indexing(ctx, mdService, baseTableDescr)
			if indexingErr != nil {
				log.Fatal(indexingErr)
			}
		}
	case "discovery":
		{
			var err error
			if baseTableTag == "" {
				log.Fatal("Base table tag has not been specified")
			}
			if lakeTableTag == "" {
				log.Fatal("Lake table tag has not been specified")
			}
			baseTableDescr, found := dumpDesc.TaggedTable(baseTableTag)
			if !found {
				log.Fatalf("Table with %v tag has not been found", baseTableTag)
			}
			discoveryStartTime := time.Now()
			lakeTableDescr, found := dumpDesc.TaggedTable(lakeTableTag)
			if !found {
				log.Fatalf("Table with %v tag has not been found", lakeTableTag)
			}
			var tx *sql.Tx
			tx, err = db.Begin()
			if err != nil {
				logger.Fatalf("%+v", err)
			}
			_, err = tx.Exec(`delete from grp_blank_cols`)
			if err != nil {
				logger.Fatalf("%+v", err)
			}
			_, err = tx.Exec(`delete from grp_jnt_cols`)
			if err != nil {
				logger.Fatalf("%+v", err)
			}
			_, err = tx.Exec(`delete from grp_jnts`)
			if err != nil {
				logger.Fatalf("%+v", err)
			}
			_, err = tx.Exec(`delete from grps`)
			if err != nil {
				logger.Fatalf("%+v", err)
			}
			err = tx.Commit()
			if err != nil {
				logger.Fatalf("%+v", err)
			}

			if len(baseTableDescr.DumpFile) > 0 {

			} else if len(baseTableDescr.DbDescFile) > 0 {
				baseTableCreds := &meta.DbCredsDesc{}
				log.Printf("Reading base table datasource configuration...")
				err = baseTableCreds.Load(baseTableDescr.DbDescFile)
				if err != nil {
					log.Fatal(err)
				}
				err = baseTableCreds.DetermineDumpingColumns(baseTableDescr)
				if err != nil {
					log.Fatal(err)
				}
			}
			baseStorageDir, err := baseTableDescr.StorageDir()
			if err != nil {
				log.Fatal(err)
			}
			/*{

				table, err := meta.ReadFromTableFile(baseStorageDir)
				if err != nil {
					err = errors.WithStack(err)
					log.Fatal(err)
				}
				err = mdService.MergeTable(ctx, table)
				if err != nil {
					err = errors.WithStack(err)
					log.Fatal(err)
				}
				columns, err := meta.ReadFromColumnFile(baseStorageDir)
				if err != nil {
					log.Fatal(err)
				}

				err = mdService.MergeColumns(ctx, table.ID, columns)
				if err != nil {
					log.Fatal(err)
				}
			}*/

			//*** BASE SIDE ****
			var baseTableMetaCache *meta.TableMetaCache
			{
				baseTable, err := mdService.FindFirstTableByDatabaseAndSchemaAndName(ctx,
					baseTableDescr.DatabaseName,
					baseTableDescr.SchemaName,
					baseTableDescr.TableName,
				)

				if err != nil {
					log.Fatal(err)
				}

				baseTableMetaCache = meta.NewTableMetaCache(mdService, baseTable.ID)
			}

			log.Printf("Loading base table index data...")
			rnsf := iixs.NewRnStorageFactory()
			dataIndexingStorage := iixs.NewMemIndexerStorage(rnsf)
			err = meta.ReadFromIndexStorageFile(dataIndexingStorage, baseStorageDir, meta.DataIndexStorageFileName)
			if err != nil {
				log.Fatal(err)
			}
			ixf := iix.NewFinder(dataIndexingStorage)

			dataSparsenessStorage := iixs.NewDataSparnessStorage(rnsf)
			err = meta.ReadFromIndexStorageFile(dataSparsenessStorage, baseStorageDir, meta.DataSparsnessStorageFileName)
			if err != nil {
				log.Fatal(errors.Unwrap(err))
			}

			//TODO: to be used while aggregating?
			ick := iix.NewDataSparsnessChecker(dataSparsenessStorage)
			_ = ick

			//*** LAKE SIDE ****
			var lakeTableMetaCache *meta.TableMetaCache
			{
				lakeTable, err := mdService.FindFirstTableByDatabaseAndSchemaAndName(ctx,
					lakeTableDescr.DatabaseName,
					lakeTableDescr.SchemaName,
					lakeTableDescr.TableName,
				)
				if err != nil {
					log.Fatal(err)
					//return err
				}
				lakeTableMetaCache = meta.NewTableMetaCache(mdService, lakeTable.ID)
			}

			valService := iix.NewColumnValueService(
				&iix.NormalizeStrategyFactory{},
			)
			cgsMap := make(iix.ColumnGroupMap)

			discoveryStrategy := iix.NewDiscoveryingRowStrategy(cgsMap, lakeTableMetaCache,
				valService, baseTableMetaCache, ixf, anchorUniqueness,
				anchorMinCount, anchorMaxCount,
			)

			if len(lakeTableDescr.DumpFile) > 0 {
				{
					cs, err := lakeTableMetaCache.Columns(context.TODO())
					if err != nil {
						//return err
						log.Fatal(err)
					}
					err = valService.RegisterAll(cs)
					if err != nil {
						//return err
						log.Fatal(err)
					}
				}
				rowHandlerAdapter := meta.NewTextRowHandlerAdapter(
					discoveryStrategy,
					lakeTableDescr,
				)
				logger.Info("Opening dump file")
				{
					fl, err := os.OpenFile(lakeTableDescr.DumpFile, os.O_RDONLY, 0x444)
					if err != nil {
						logger.Fatal(err)
					}
					logger.Info("Start column group discovery. Reading data from the dump file...")
					_, err = sources.TextStream(ctx, fl, rowHandlerAdapter)
					if err != nil {
						logger.Fatal(err)
					}
				}

			} else if len(lakeTableDescr.DbDescFile) > 0 {
				lakeDbCreds := &meta.DbCredsDesc{}
				logger.Info("Reading lake table datasrource configuration...")
				err = lakeDbCreds.Load(lakeTableDescr.DbDescFile)
				if err != nil {
					log.Fatal(err)
				}
				err = lakeDbCreds.DetermineDumpingColumns(lakeTableDescr)
				if err != nil {
					//return err
					log.Fatal(err)
				}
				{
					cs, err := lakeTableMetaCache.Columns(context.TODO())
					if err != nil {
						//return err
						log.Fatal(err)
					}
					err = valService.RegisterAll(cs)
					if err != nil {
						//return err
						log.Fatal(err)
					}
				}

				dumpingQuery := lakeTableDescr.GetDumpingQuery()
				logger.Info("Start column group discovery. Requesting data from the database...")
				var rowCount int
				err = lakeDbCreds.RunQuery(
					func(rows *sql.Rows) (err error) {
						log.Println("Discovery in query result set")
						rowCount, err = sources.SqlRowsStream(ctx, rows, discoveryStrategy)
						return err
					},
					dumpingQuery,
				)
				if err != nil {
					log.Fatal(err)
				}
				{
					lakeTable, err := lakeTableMetaCache.Table(ctx)
					if err != nil {
						log.Fatal(err)
					}
					lakeTable.DumpingQuery = sql.NullString{dumpingQuery, true}
					lakeTable.RowCount = sql.NullInt64{int64(rowCount), true}
				}
			}
			///			cgsMap.PrintResult(meta.ColumnNamesFromIDs)
			/*fout, err := os.Create(discoveryFile)
			if err != nil {
				log.Fatal(err)
			}
			defer fout.Close()
			_, err = discoveryStrategy.WriteTo(fout)
			if err != nil {
				log.Fatal(err)
			}*/
			// db, err := sql.Open("sqlite3", sqlDbFile)
			// if err != nil {
			// 	logger.Fatal(err)
			// }
			// defer db.Close()
			err = iix.CgSave(ctx, db, lakeTableTag, baseTableTag, 1, 1, cgsMap)
			if err != nil {
				log.Fatal(err)
			}
			lakeTableMetaCache.SaveAll(ctx)
			if err != nil {
				log.Fatal(err)
			}
			logger.Infof("Finish discory. Found %v column groups. Total time: %v", len(cgsMap),
				time.Since(discoveryStartTime))
		}
	case "aggregation":
		{
			rnsf := iixs.NewRnStorageFactory()

			/*			fr, err := os.Open(discoveryFile)
						if err != nil {
							log.Fatal(err)
						}
						defer fr.Close()
			*/
			baseTableDescr, found := dumpDesc.TaggedTable(baseTableTag)
			if !found {
				log.Fatalf("Table with %v tag has not been found", baseTableTag)
			}
			baseStorageDir, err := baseTableDescr.StorageDir()
			if err != nil {
				log.Fatal(err)
			}
			dataSparsenessStorage := iixs.NewDataSparnessStorage(rnsf)
			err = meta.ReadFromIndexStorageFile(dataSparsenessStorage, baseStorageDir, meta.DataSparsnessStorageFileName)
			if err != nil {
				log.Fatal(errors.Unwrap(err))
			}

			//sqliteRegister("sqlite3_app");
			db, err := sql.Open("sqlite3", wc.SqliteConnString)
			//_journal_mode
			if err != nil {
				logger.Fatal(err)
			}
			defer db.Close()

			baseTable, err := mdService.FindFirstTableByDatabaseAndSchemaAndName(ctx,
				baseTableDescr.DatabaseName,
				baseTableDescr.SchemaName,
				baseTableDescr.TableName,
			)
			if err != nil {
				log.Fatal(err)
			}

			//TODO: to be used while aggregating?
			ick := iix.NewDataSparsnessChecker(dataSparsenessStorage)
			agg := new(iix.ColumnGroupAggregator)
			agg.Rnsf = rnsf
			agg.Bck = ick
			agg.BaseRowMinCount = int(math.Round(baseRowMinPct * float64(baseTable.RowCount.Int64) / 100))
			agg.MergingThreadCount = wc.AggMergeThreadCount
			agg.PersistingThreadCount = wc.AggPersistingThreadCount
			agg.PersistingBatchSize = wc.AggPersistingBatchSise
			agg.BsCacheSize = wc.AggBsCacheSise

			agg.MergeBlanks = true

			agg.Aggregate(db)

			/*
				fa := func(a string, b []string) (ba []string, err error) {
					return b, nil
				}
				_ = fa

			*/

			//agg.ReadFrom(fr)
			//agg.Aggregate2(meta.ColumnNamesFromIDs)

			//agg.Aggregate(minJoints, fa)
		}
	default:
		flag.PrintDefaults()

		log.Fatalf("\n\nunknown mode: %v", mode)
	}

}
