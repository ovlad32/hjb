package iix

import "fmt"

type charSeq struct {
	maxIdValid bool
	maxId      int
	ids        map[int]bool
}

func newCharSeq() *charSeq {
	return &charSeq{}
}
func newCharSeqFrom(s string, oneChar byte) *charSeq {
	r := &charSeq{}
	bs := []byte(s)
	for id, bit := range bs {
		if bit != oneChar {
			continue
		}
		if r.ids == nil {
			r.ids = make(map[int]bool)
		}
		r.ids[id] = true
		if r.maxId < id {
			r.maxId = id
			r.maxIdValid = true
		}
	}
	return r
}

func (a *charSeq) Add(id int) {
	if id <= 0 {
		panic(fmt.Sprintf("invalid id value:%v", id))
	}
	if a.maxId < id {
		a.maxId = id
		a.maxIdValid = true
	}
	if a.ids == nil {
		a.ids = make(map[int]bool)
	}
	a.ids[id] = true
}

func (a *charSeq) Remove(id int) {
	if id <= 0 {
		panic(fmt.Sprintf("invalid id value:%v", id))
	}
	delete(a.ids, id)
	a.maxIdValid = false
}

func (a charSeq) String(oneChar byte) string {
	if !a.maxIdValid {
		for id := range a.ids {
			if a.maxId < id {
				a.maxId = id
				a.maxIdValid = true
			}
		}
	}
	if a.maxId <= 0 {
		return ""
	}
	r := make([]byte, a.maxId)
	for i := 0; i < a.maxId; i++ {
		r[i] = '0'
	}
	for id := range a.ids {
		r[id-1] = oneChar
	}
	return string(r)
}
