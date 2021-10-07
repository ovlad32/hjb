package iix

import (
	"context"

	"github.com/ovlad32/hjb/meta"
	"github.com/pkg/errors"
)

/*type Value = string

type IValueService interface {
	ToValue(rawValue string) Value
	GroupBy(columns []meta.Column, rawValues []string)  map[string][]meta.Column
	Normalize(columnId meta.Column,value string) (func (rawValue string) string, error)
}
*/
type iNormalizer interface {
	Normalize(context.Context, *meta.Column, string) ([]string, []bool, error)
}

type iNormalizeStrategy interface {
	Normalize(string) ([]string, []bool, error)
}
type iNormalizeStrategyFactory interface {
	Create(*meta.Column) ([]iNormalizeStrategy, error)
}

type columnKey = string

func keyOf(c *meta.Column) columnKey {
	return c.ID.String
}

type strategyMap map[columnKey][]iNormalizeStrategy

type ValueService struct {
	f   iNormalizeStrategyFactory
	sgm strategyMap
}

func NewColumnValueService(f iNormalizeStrategyFactory) ValueService {
	result := ValueService{
		f:   f,
		sgm: make(strategyMap),
	}
	return result
}

func (vs ValueService) Register(c *meta.Column) (err error) {
	nss, err := vs.f.Create(c)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	if len(nss) > 0 {
		vs.sgm[keyOf(c)] = nss
	}
	return
}
func (vs ValueService) RegisterAll(cs []*meta.Column) (err error) {
	for i := range cs {
		err = vs.Register(cs[i])
		if err != nil {
			err = errors.WithStack(err)
			return
		}
	}
	return
}

func (vs ValueService) Normalize(ctx context.Context, column *meta.Column, value string) (normalized []string, inclusionFlags []bool, err error) {
	values := []string{value}
	inclusions := []bool{true}
	strategies, found := vs.sgm[keyOf(column)]
	if !found {
		normalized = values
		inclusionFlags = inclusions
		return
	}

	for _, strategy := range strategies {
		pValues := make([]string, 0, len(values))
		pInclusions := make([]bool, 0, len(inclusions))

		for _, rawValue := range values {
			retValues, retInclusions, erre := strategy.Normalize(rawValue)
			if erre != nil {
				err = errors.WithStack(erre)
				return
			}
			pValues = append(pValues, retValues...)
			pInclusions = append(pInclusions, retInclusions...)
		}
		values = pValues
		inclusions = pInclusions
	}
	normalized = values
	inclusionFlags = inclusions
	return
}
