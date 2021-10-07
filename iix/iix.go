package iix

import (
	"io"

	"github.com/ovlad32/hjb/iixs"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)


var logger *log.Logger

func SetLogger(l *log.Logger) {
	logger = l
}

var queryTexts map[string]string

//func init() { 	}
func init() {
	queryTexts = make(map[string]string)
}

type IIndexingStrategy interface {
	Index(value string, rowNumber int, columns []string) error
}
type IStrategies interface {
	Register(ist IIndexingStrategy) (int, error)
}
type IIndexer interface {
	IStrategies
	IIndexingStrategy
}

type IEmptyRowCollector interface {
	Collect(int, string) error
}

type IVcStroage interface {
	Add(columnId string, value string) error
	Cardinality(string) (uint, error)
	io.ReaderFrom
	io.WriterTo
}

type ISpStorage interface {
	Add(string, int) error
	CheckRowNumber(int) (map[string]bool, error)
	CheckColumn(string, int) (bool, error)
	io.ReaderFrom
	io.WriterTo
}

type Indexer struct {
	strategies []IIndexingStrategy
}

type IFinder interface {
	//IStrategies
	Find(value string) (iixs.Rcs, bool, error)
}

type IBlankChecker interface {
	CheckRowNumber(int) (map[string]bool, error) //??
	CheckColumn(string, int) (bool, error)
}

func NewIndexer() *Indexer {
	return &Indexer{}
}

func (ixr *Indexer) Register(ist IIndexingStrategy) (n int, err error) {
	if ixr.strategies == nil {
		ixr.strategies = make([]IIndexingStrategy, 0, 10)
	}
	ixr.strategies = append(ixr.strategies, ist)
	n = len(ixr.strategies)
	return
}
func (ixr Indexer) Index(value string, rowNumber int, columns []string) (err error) {
	for i := range ixr.strategies {
		err = ixr.strategies[i].Index(value, rowNumber, columns)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
	}
	return
}

//*************************************************************************

type VrcIndexingStrategy struct {
	rcStorage iixs.IRcStorage
}

func NewVrcIndexingStrategy(s iixs.IRcStorage) *VrcIndexingStrategy {
	return &VrcIndexingStrategy{s}
}

//TODO: redesign it
func NewFinder(s iixs.IRcStorage) IFinder {
	return &VrcIndexingStrategy{s}
}

func (st *VrcIndexingStrategy) Index(value string, rowNumber int, columns []string) (err error) {
	rcs, txFunc, err := st.rcStorage.Update(value)
	if err != nil {
		if txFunc != nil {
			txFunc(iixs.ROLLBACK)
		}
		err = errors.WithStack(err)
		return err
	}

	for i := range columns {
		err = rcs.Append(rowNumber, columns[i])
		if err != nil {
			err = errors.WithStack(err)
			return
		}
	}

	err = txFunc(iixs.COMMIT)(rcs)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	return
}

func (ix VrcIndexingStrategy) Find(value string) (rcs iixs.Rcs, found bool, err error) {
	rcs, found, err = ix.rcStorage.Find(value)
	return
}

//*************************************************************************

type DataCardinalityStrategy struct {
	vcStorage IVcStroage
}

func NewDataCardinalityStrategy(s IVcStroage) *DataCardinalityStrategy {
	return &DataCardinalityStrategy{
		vcStorage: s,
	}
}
func (s *DataCardinalityStrategy) Index(value string, rowNumber int, columns []string) (err error) {
	if value == "" {
		return
	}
	for i := range columns {
		err = s.vcStorage.Add(columns[i], value)
		if err != nil {
			err = errors.WithStack(err)
		}
	}
	return
}

//*************************************************************************
type DataSparnessStrategy struct {
	spasnessStorage ISpStorage
}
type dataSparsnessChecker = DataSparnessStrategy

func NewDataSparnessStrategy(s ISpStorage) *DataSparnessStrategy {
	return &DataSparnessStrategy{
		spasnessStorage: s,
	}
}
func NewDataSparsnessChecker(s ISpStorage) *dataSparsnessChecker {
	return &DataSparnessStrategy{spasnessStorage: s}
}
func (s *DataSparnessStrategy) Index(value string, rowNumber int, columns []string) (err error) {
	if value != "" {
		return
	}
	for i := range columns {
		erre := s.spasnessStorage.Add(columns[i], rowNumber)
		if erre != nil {
			err = errors.WithStack(erre)
		}
	}
	return
}
func (c *DataSparnessStrategy) CheckRowNumber(rowNumber int) (map[string]bool, error) {
	return c.spasnessStorage.CheckRowNumber(rowNumber)
}

func (c *DataSparnessStrategy) CheckColumn(columnId string, rowNumber int) (found bool, err error) {
	return c.spasnessStorage.CheckColumn(columnId, rowNumber)
}

//func (s *dataSparnessStorage) CheckColumn(columnId string, rowNumber int) (found bool, err error) {
