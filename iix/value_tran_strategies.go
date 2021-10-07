package iix

import (
	"strings"

	"github.com/ovlad32/hjb/meta"
)

/*******************************************************/
var oneTrueSlice = []bool{true}
var oneFalseSlice = []bool{false}

type LeadingCharTransformationStrategy struct {
	leadingChar string
}

func (st LeadingCharTransformationStrategy) Normalize(value string) ([]string, []bool, error) {
	if st.leadingChar != "" {
		value = strings.TrimLeft(value, st.leadingChar)
	}
	return []string{value}, oneTrueSlice, nil
}

type SplitTransformationStrategy struct {
	splitSeparator string
}

func (st SplitTransformationStrategy) Normalize(value string) ([]string, []bool, error) {
	if st.splitSeparator == "" {
		return []string{value}, oneTrueSlice, nil
	}
	return strings.Split(value, st.splitSeparator), oneTrueSlice, nil
}

/*******************************************************/
type StopWordsTransformationStrategy struct {
	caseSensitive bool
	stopWords     map[string]bool
}

func (st StopWordsTransformationStrategy) Normalize(value string) ([]string, []bool, error) {
	testedValue := strings.TrimSpace(value)
	if !st.caseSensitive {
		testedValue = strings.ToLower(testedValue)
	}
	_, found := st.stopWords[testedValue]
	if found {
		return []string{value}, oneFalseSlice, nil
	}
	return []string{value}, oneTrueSlice, nil
}

func NewStopWordTransformationStrategy(caseSensitive bool, words []string) (result StopWordsTransformationStrategy) {
	result.caseSensitive = caseSensitive
	result.stopWords = make(map[string]bool)
	for _, word := range words {
		if !caseSensitive {
			word = strings.ToLower(word)
		}
		result.stopWords[word] = true
	}
	return
}

type NormalizeStrategyFactory struct {
}

func (f NormalizeStrategyFactory) Create(c *meta.Column) (strategies []iNormalizeStrategy, err error) {
	add := func(s iNormalizeStrategy) {
		if strategies == nil {
			strategies = make([]iNormalizeStrategy, 0, 3) //TODO: 3 strategies so far
		}
		strategies = append(strategies, s)
	}

	if c.FusionSeparator != "" {
		add(
			SplitTransformationStrategy{
				splitSeparator: c.FusionSeparator,
			},
		)
	}
	if c.LeadingChar != "" {
		add(LeadingCharTransformationStrategy{
			leadingChar: c.LeadingChar,
		})
	}
	if c.StopWords != "" {
		add(NewStopWordTransformationStrategy(false, strings.Split(c.StopWords, ",")))
	}
	return
}
