package harbour

import (
	"time"
)

type (
	DeathReason uint

	//DeathNote messags sent on exit to parent.
	DeathNote struct {
		//Reason of death.
		Reason DeathReason
		//Err
		Err string
	}

	Consumer interface {
		Start()
		Dead() <-chan DeathNote
		Stop(time.Duration) error
	}
)
