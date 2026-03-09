package domain

func check(cond bool) {
	if !cond {
		panic("assertion failed")
	}
}
