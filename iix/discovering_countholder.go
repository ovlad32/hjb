package iix

import (
	"fmt"
	"sync"
)

type counter struct {
	mx  sync.Mutex
	seq int
}

func (c *counter) next() int {
	c.mx.Lock()
	c.seq++
	c.mx.Unlock()
	return c.seq
}

type countHolder struct {
	base      counter
	lake      counter
	blankBase counter
	cgs       counter
}

func (c *countHolder) nextGroupSeqId() string {
	return fmt.Sprintf("CG%v", c.cgs.next())
}

func (c *countHolder) nextBaseSeqId() int {
	return c.base.next()
}
func (c *countHolder) nextBlankBaseId() int {
	return c.blankBase.next()
}
func (c *countHolder) nextLakeSeqId() int {
	return c.lake.next()
}

type iSeqProvider interface {
	nextGroupSeqId() string
	nextBaseSeqId() int
	nextLakeSeqId() int
}

type columnSeqStorage struct {
	baseMap map[string]int
	lakeMap map[string]int
	seqGen iSeqProvider
}

//new(countHolder)
// TODO: Make it thread-safe
func newColumnSeqStorage(gen iSeqProvider) *columnSeqStorage {
	return &columnSeqStorage{
		baseMap: make(map[string]int),
		lakeMap: make(map[string]int),
		seqGen:  gen,
	}

}

type iColumnSeqMapper interface {
	mapLakeColumnID(c string) (i int)
	mapBaseColumnID(c string) (i int)
}

func (h *columnSeqStorage) mapLakeColumnID(c string) (i int) {
	var found bool
	if i, found = h.lakeMap[c]; !found {
		i = h.seqGen.nextLakeSeqId()
		h.lakeMap[c] = i
	}
	return i
}
func (h *columnSeqStorage) mapBaseColumnID(c string) (i int) {
	var found bool
	if i, found = h.baseMap[c]; !found {
		i = h.seqGen.nextBaseSeqId()
		h.baseMap[c] = i
	}
	return i
}
