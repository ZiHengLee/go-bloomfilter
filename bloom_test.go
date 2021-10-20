package go_bloomfilter

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// TestAddAndGetSync test concurrent write and read in filter
func TestAddAndGetSync(t *testing.T) {
	sizeData := 100000
	stringLen := 30
	parts := 10

	filter := New(uint64(sizeData), 3)
	// concurrent write and read
	fn := func(size int, wg *sync.WaitGroup) {
		defer wg.Done()
		m := make(map[string]struct{}, size)
		for i := 0; i < size; i++ {
			randStr := randStringRunes(stringLen)
			// add unique random string
			if _, ok := m[randStr]; !ok {
				m[randStr] = struct{}{}
				// write
				filter.AddString(randStr)
				// read
				exist := filter.ExistsString(randStr)
				if !exist {
					t.Errorf("key %s not exist", randStr)
				}
			}
		}
	}
	var waitGroup sync.WaitGroup
	for i := 0; i < parts; i++ {
		waitGroup.Add(1)
		go fn(sizeData/parts, &waitGroup)
	}
	waitGroup.Wait()
}
