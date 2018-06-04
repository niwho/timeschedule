package common

import (
	"fmt"
	"testing"
	"time"
)

func TestSched(t0 *testing.T) {
	InitTimeoutSchedule()

	t := time.Now()
	id1 := TimeoutScheduleIns.AddCheckJob(5*time.Second, func() bool {
		fmt.Println(time.Now(), t.Add(5*time.Second), "$$$$ 5 done")
		return true
	})

	rid := TimeoutScheduleIns.AddCheckJob(15*time.Second, func() bool {
		fmt.Println(time.Now(), t.Add(15*time.Second), "$$$$anothor 15 done")
		return true
	})
	//time.Sleep(10 * time.Second)
	id2 := TimeoutScheduleIns.AddCheckJob(2*time.Second, func() bool {
		fmt.Println(time.Now(), t.Add(2*time.Second), "$$$$anothor 5 done, false======")
		return false
	})
	fmt.Println(id1, rid, id2)

	k := 0
	for {
		select {
		case <-time.After(time.Second):
			fmt.Println(time.Now().UnixNano() / 1e6)
			k += 1
			switch k {
			case 10:
				TimeoutScheduleIns.RemoveCheckJob(rid)
			case 11:
				t := time.Now()
				TimeoutScheduleIns.AddCheckJob(5*time.Second, func() bool {
					fmt.Println(time.Now(), t.Add(5*time.Second), "@@@@@@@@@@@@@@@@$$$$anothor 5 done")
					return true
				})
			case 12:
				t := time.Now()
				TimeoutScheduleIns.AddCheckJob(50*time.Second, func() bool {
					fmt.Println(time.Now(), t.Add(50*time.Second), "@@@@@@@@@@@@@@@@$$$$anothor 50 done")
					return true
				})
			}
		}
	}
}
