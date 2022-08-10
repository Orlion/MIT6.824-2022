package findk

import "testing"

func TestInts(t *testing.T) {
	type input struct {
		a []int
		k int
	}
	intsTests := []struct {
		input    input
		expected int
	}{
		{input{[]int{1}, 1}, 1},
		{input{[]int{1, 2}, 1}, 1},
		{input{[]int{1, 2}, 2}, 2},
		{input{[]int{1, 2, 3, 4, 5, 6}, 3}, 3},
		{input{[]int{2, 1}, 1}, 1},
		{input{[]int{2, 1}, 2}, 2},
		{input{[]int{6, 5, 4, 3, 2, 1}, 3}, 3},
		{input{[]int{1, 39, 2, 3, 10}, 5}, 39},
	}

	for _, v := range intsTests {
		if Ints(v.input.a, v.input.k) != v.expected {
			t.Errorf("Ints(%v, %d) = %d, not %d \n", v.input.a, v.input.k, Ints(v.input.a, v.input.k), v.expected)
		}
	}
}
