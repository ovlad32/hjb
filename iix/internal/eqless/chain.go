package eqless

type NextFunc func(int, int) bool

func Chain(
	weightFunc func(int) int,
	nextWith NextFunc,
) NextFunc {
	return NextFunc(func(i int, j int) bool {
		ir := weightFunc(i)
		jr := weightFunc(j)
		if ir == jr {
			return nextWith(i, j)
		}
		return ir < jr
	})
}
