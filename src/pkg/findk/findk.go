package findk

// 从[]int中寻找第k大的数
func Ints(a []int, k int) int {
	if k < 1 {
		panic("k不能小于1")
	}
	length := len(a)
	if length < k {
		panic("a's length不能小于k")
	}

	if length == 1 {
		return a[0]
	}

	var left, right []int
	for k := 1; k < len(a); k++ {
		if a[k] > a[0] {
			right = append(right, a[k])
		} else {
			left = append(left, a[k])
		}
	}

	if len(left) >= k {
		return Ints(left, k)
	} else if len(left) == k-1 {
		return a[0]
	} else {
		return Ints(right, k-len(left)-1)
	}
}
