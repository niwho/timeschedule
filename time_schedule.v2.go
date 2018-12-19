package timeschedule

import (
	"fmt"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/ryszard/goskiplist/skiplist"
)

type JobContextV2 interface {
	SetStartTime(int64) //毫秒数
	SetStatus(int)

	SetNextRunTime(int64) //毫秒数
	GetNextRunTime() int64

	GetRunCount() int64
	SetRunCount(int64)

	// 外部id, snowflake
	SetJobID(int64)
	GetJobID() int64
}

type JobV2 struct {
	JobContextV2
	InnerID JobID
	cb      func(JobContextV2) bool
}

var TimeoutScheduleInsV2 *TimeoutScheduleV2

type TimeoutScheduleV2 struct {
	*skiplist.SkipList
	*snowflake.Node
	addCh    chan *JobV2
	removeCh chan struct {
		oid    int64
		ignore bool
	}
	index map[int64]*JobV2
	debug struct {
		d time.Duration
	}

	// JobV2 状态变化跟踪
	updateCh chan JobContextV2
	UpdataCb func(JobContextV2) // 注意这个回调函数长时间阻塞可能会导致整个job调度停滞
}

func InitTimeoutScheduleV2() {
	idf, _ := snowflake.NewNode(100)
	TimeoutScheduleInsV2 = &TimeoutScheduleV2{
		SkipList: skiplist.New(),
		Node:     idf,
		addCh:    make(chan *JobV2, 128),
		updateCh: make(chan JobContextV2, 128),
		removeCh: make(chan struct {
			oid    int64
			ignore bool
		}, 128),
		index: map[int64]*JobV2{},
	}
	go TimeoutScheduleInsV2.Start()
}

func (ts *TimeoutScheduleV2) updateJob() {
	for {
		select {
		case jd := <-ts.updateCh:
			if ts.UpdataCb != nil {
				ts.UpdataCb(jd)
			}
		}
	}
}

func (ts *TimeoutScheduleV2) AddCheckJob(jc JobContextV2, cb func(jc JobContextV2) bool) int64 {
	jd := JobID{
		ID: jc.GetNextRunTime(),
		// 冲突项
		SEQ: 0,
	}
	jc.SetStartTime(jc.GetNextRunTime())
	outId := jc.GetJobID()
	if jc.GetJobID() == 0 {
		outid = int64(ts.Generate())
		jc.SetJobID(outid)
	}
	job := &JobV2{
		JobContextV2: jc,
		InnerID:      jd,
		cb:           cb,
	}
	// todo: with timeout
	ts.updateJobStatus(job, WAITING)

	ts.addCh <- job
	// JobID maybe modify
	// chan 是引用传递方式，所以可以这样使用
	//jd.SEQ = <-job.ch
	return outid
}

func (ts *TimeoutScheduleV2) updateJobStatus(job *JobV2, status JobStatue) {
	job.SetStatus(int(status))
	ts.updateCh <- job.JobContextV2
}

func (ts *TimeoutScheduleV2) RemoveCheckJob(jid int64) {
	ts.removeCh <- struct {
		oid    int64
		ignore bool
	}{jid, false}
}

type jobDoneV2 struct {
	*JobV2
	result bool
}

func (ts *TimeoutScheduleV2) process(clock *time.Timer, doWorks chan jobDoneV2) {
	now := time.Now().UnixNano() / 1e6
	var dropItems []interface{}
	it := ts.Iterator()
	for it.Next() {
		if job, ok := it.Value().(*JobV2); ok {
			if t := job.InnerID.ID - now; t <= 0 {
				if job.cb == nil {
					job.cb = func(JobContextV2) bool { return true }
				}
				go func() {
					// 控制并发度，并处理结果, false 的要重新reschedule
					ts.updateJobStatus(job, RUNING)
					job.SetRunCount(job.GetRunCount() + 1)
					doWorks <- jobDoneV2{job, job.cb(job.JobContextV2)}
				}()
				dropItems = append(dropItems, it.Key())
			} else {
				// 重置timer会有很大的开销么，是否还不如定时轮询
				if !clock.Stop() {
					//<-clock.C
				}
				ts.debug.d = time.Duration(t * 1e6)
				clock.Reset(time.Duration(t * 1e6))
				break
			}

		} else {
			fmt.Errorf("type err: job")
			dropItems = append(dropItems, it.Key())
			continue
		}

	}
	// important 同步删除
	for _, key := range dropItems {
		ts.Delete(key)
	}
}
func (ts *TimeoutScheduleV2) Start() {
	clock := time.NewTimer(365 * 24 * time.Hour)
	doWorks := make(chan jobDoneV2, 1024)
	go ts.updateJob()
	go func() {
		for {
			select {
			case jd := <-doWorks:
				if !jd.result {
					//fmt.Println("*************", jd.JobV2.Retry, jd.JobV2.MaxRetry, jd.JobV2.Retry <= jd.JobV2.MaxRetry)
					d := jd.GetNextRunTime() - time.Now().UnixNano()/1e6
					// retry
					if d > 0 {
						jd.InnerID.ID = jd.GetNextRunTime()
						jd.SetRunCount(jd.GetRunCount() + 1)

						ts.updateJobStatus(jd.JobV2, WAITING)

						ts.addCh <- jd.JobV2
					} else {
						ts.removeCh <- struct {
							oid    int64
							ignore bool
						}{jd.GetJobID(), true}
					}

				} else {
					//fmt.Println(time.Now(), jd.JobV2.OutID)
					ts.removeCh <- struct {
						oid    int64
						ignore bool
					}{jd.GetJobID(), true}
				}
			}
		}
	}()
	for {
		select {
		case outID := <-ts.removeCh:
			if job, found := ts.index[outID.oid]; found {

				if outID.ignore {
					ts.updateJobStatus(job, DONE)
				} else {
					ts.updateJobStatus(job, CANCEL)
				}
				if _, ok := ts.Delete(job.InnerID); ok {
				}
				delete(ts.index, outID.oid)
			}
		case job := <-ts.addCh:
			if job.GetRunCount() > 0 {
				// 说明已经被主动删除了
				if _, found := ts.index[job.GetJobID()]; !found {
					break
				}
			}
			it := ts.Seek(job.InnerID)
			if it == nil {
				ts.Set(job.InnerID, job)
			} else {
				preJob := it.Value().(*JobV2)
				seq := 0
				for preJob.InnerID.ID == job.InnerID.ID {
					seq = preJob.InnerID.SEQ
					it.Next()
					preJob = it.Value().(*JobV2)
				}
				seq += 1
				job.InnerID.SEQ = seq
				ts.Set(job.InnerID, job)
			}
			ts.index[job.GetJobID()] = job
		case <-clock.C:
			// 如果没有任何job了， reset 1year
			if ts.Len() <= 0 {
				//fmt.Println("36555555")
				if !clock.Stop() {
					//<-clock.C
				}
				ts.debug.d = 365 * 24 * time.Hour
				clock.Reset(365 * 24 * time.Hour)
			}
		}
		ts.process(clock, doWorks)
		//fmt.Println("monitor", len(ts.index), ts.Len(), ts.debug.d)
		//for k, v := range ts.index {
		//	fmt.Println(k, v)
		//}
		//fmt.Println("mapppp end")
	}
}
