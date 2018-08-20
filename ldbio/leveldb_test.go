package ldbio

import (
	"testing"
	"fmt"
	"time"
)

func Test_leveldb1(t *testing.T) {
	ldb := &LevelDBMser{
		filepath: "./testldb",
	}
	ldb.Init()

	startW := time.Now()
	for i := 0; i < 10000000; i++ {
		ldb.Put([]byte(string(i)), []byte(string(i+100)))
	}
	endW := time.Now()

	chW := endW.Sub(startW)
	fmt.Println("W ======", chW)

	startR := time.Now()
	for i := 0; i < 10000000; i ++ {
		val, err := ldb.Get([]byte(string(i)))
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(val)
	}
	endR := time.Now()
	chR := endR.Sub(startR)
	fmt.Println("R ======", chR)
}
