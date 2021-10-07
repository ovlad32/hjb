package misc

import "strings"

func JoinSlice(sliceLen int, sep string, f func(int) string) string {
	switch sliceLen {
	case 0:
		return ""
	case 1:
		return f(1)
	}
	n := len(sep) * (sliceLen - 1)
	ss := make([]string, sliceLen)
	for i := 0; i < sliceLen; i++ {
		ss[i] = f(i)
		n += len(ss[i])
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteString(ss[0])
	for i := 1; i < sliceLen; i++ {
		b.WriteString(sep)
		b.WriteString(ss[i])
	}
	return b.String()
}

func BuildMapKey(is, hs []string, sep string) string {
	return strings.Join([]string{
		strings.Join(is, sep),
		"-",
		strings.Join(hs, sep),
	}, sep)
}
