package timeschedule

import (
	"fmt"
	"testing"
	"time"
)

func TestSched(t0 *testing.T) {
	InitTimeoutSchedule()

	t := time.Now()
	id1 := TimeoutScheduleIns.AddCheckJob(5*time.Second, func(JobContext) bool {
		fmt.Println(time.Now(), t.Add(5*time.Second), "$$$$ 5 done")
		return true
	})

	t = time.Now()
	rid := TimeoutScheduleIns.AddCheckJob(15*time.Second, func(JobContext) bool {
		fmt.Println(time.Now(), t.Add(15*time.Second), "$$$$anothor 15 done, remove")
		return true
	})
	//time.Sleep(10 * time.Second)
	t = time.Now()
	id2 := TimeoutScheduleIns.AddCheckJob(2*time.Second, func(jc JobContext) bool {
		fmt.Println(time.Now(), t.Add(2*time.Second), "reapted $$$$anothor 5 done, false======")
		t := time.Now().Add(time.Second * 5)
		fmt.Println("reaped expected at: ", t, jc.GetRunCount())
		jc.SetNextRunTime(t)
		return false
	})
	fmt.Println(id1, rid, id2)

	k := 0
	for {
		select {
		case <-time.After(time.Second):
			//fmt.Println(time.Now().UnixNano() / 1e6)
			k += 1
			switch k {
			case 10:
				TimeoutScheduleIns.RemoveCheckJob(rid)
			case 11:
				t := time.Now()
				TimeoutScheduleIns.AddCheckJob(5*time.Second, func(JobContext) bool {
					fmt.Println(time.Now(), t.Add(5*time.Second), "@@@@@@@@@@@@@@@@$$$$anothor 5 done")
					return true
				})
			case 12:
				t := time.Now()
				TimeoutScheduleIns.AddCheckJob(50*time.Second, func(JobContext) bool {
					fmt.Println(time.Now(), t.Add(50*time.Second), "@@@@@@@@@@@@@@@@$$$$anothor 50 done")
					return true
				})
			case 13:
				TimeoutScheduleIns.RemoveCheckJob(id2)
			}
		}
	}
}
