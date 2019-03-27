package pkg

import (
	"fmt"
	"github.com/honestbee/gopkg/gid"
	"time"
)

//MustGid always gets a gid.
var MustGid = func(attempts int) uint64 {
	for i := 0; i < attempts; i++ {
		if id, err := gid.Get(); err == nil {
			return id
		}
	}
	fmt.Println("[WARNING] ran out of attempts to generate gid. collision is possible")
	return uint64(time.Now().UnixNano())
}
