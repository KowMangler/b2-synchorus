package main

import (
	"fmt"
)

func main() {
	type ccheck struct {
		Name    string
		Address string
	}
	var names []string

	names = append(names, "bert")
	names = append(names, "ernie")
	names = append(names, "fozzie")
	names = append(names, "count")
	names = append(names, "elmo")

	for l := range names {
		fmt.Println("this is index starter", l)
		for s := l + 1; s < len(names); s++ {
			fmt.Println("inside nested loop", s)
		}
	}
	names = names[:len(names)-1]
	fmt.Println(names)

}
