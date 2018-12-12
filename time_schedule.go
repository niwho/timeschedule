package timeschedule

import (
	"fmt"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/ryszard/goskiplist/skiplist"
)

type JOB interface {
}

type JobID struct {
	ID  int64 // unix time 毫秒
	SEQ int
}

func (j JobID) LessThan(other skiplist.Ordered) bool {
	if jo, ok := other.(JobID); ok {
		if j.ID == jo.ID {
			return j.SEQ < jo.SEQ
		} else {
			return j.ID < jo.ID
		}
	} else if jo, ok := other.(*JobID); ok {
		if j.ID == jo.ID {
			return j.SEQ < jo.SEQ
		} else {
			return j.ID < jo.ID
		}
	}
	return true

}

type JobContext interface {
	SetNextRunTime(time.Time)
	GetRunCount() int
	SetValue(string, interface{})
	GetValue(string) (interface{}, bool)
	GetJobID() int64
	GetJobDescription() string
}

type JobStatue int

const (
	_ JobStatue = iota
	WAITING
	RUNING
	DONE
)

type Job struct {
	JobID

	OutID       int64
	Status      JobStatue
	Description string

	//ch chan int64
	runCount int
	nextTime time.Time
	cb       func(JobContext) bool

	data map[string]interface{}
}

func (j *Job) GetJobDescription() string {
	return j.Description
}

func (j *Job) GetJobID() int64 {
	return j.OutID
}

func (j *Job) SetNextRunTime(t time.Time) {
	j.nextTime = t
}

func (j *Job) GetRunCount() int {
	return j.runCount

}

func (j *Job) GetNextTime() time.Time {
	return j.nextTime

}

func (j *Job) SetValue(key string, val interface{}) {
	j.data[key] = val
}

func (j *Job) GetValue(key string) (interface{}, bool) {
	val, found := j.data[key]
	return val, found
}

type TIMESCHED interface {
	AddCheckJob(time.Duration, func() bool) int64
	RemoveCheckJob(int64)
}

var TimeoutScheduleIns *TimeoutSchedule

type TimeoutSchedule struct {
	*skiplist.SkipList
	*snowflake.Node
	addCh    chan *Job
	removeCh chan struct {
		oid    int64
		ignore bool
	}
	index map[int64]*Job
	debug struct {
		d time.Duration
	}

	// Job 状态变化跟踪
	updateCh chan *Job
	UpdataCb func(*Job) // 注意这个回调函数长时间阻塞可能会导致整个job调度停滞
}

func InitTimeoutSchedule() {
	idf, _ := snowflake.NewNode(100)
	TimeoutScheduleIns = &TimeoutSchedule{
		SkipList: skiplist.New(),
		Node:     idf,
		addCh:    make(chan *Job, 128),
		updateCh: make(chan *Job, 128),
		removeCh: make(chan struct {
			oid    int64
			ignore bool
		}, 128),
		index: map[int64]*Job{},
	}
	go TimeoutScheduleIns.Start()
}

func (ts *TimeoutSchedule) updateJob() {
	for {
		select {
		case jd := <-ts.updateCh:
			if ts.UpdataCb != nil {
				ts.UpdataCb(jd)
			}
		}
	}
}

func (ts *TimeoutSchedule) AddCheckJobWithTimeV2(t time.Time, cb func(jc JobContext) bool, desc string) int64 {
	jd := JobID{
		ID: t.UnixNano() / 1e6,
		// 冲突项
		SEQ: 0,
	}
	outid := int64(ts.Generate())
	job := &Job{JobID: jd,
		cb:          cb,
		OutID:       outid,
		Description: desc,
		data:        map[string]interface{}{},
	}
	// todo: with timeout
	ts.updateJobStatus(job, WAITING)

	ts.addCh <- job
	// JobID maybe modify
	// chan 是引用传递方式，所以可以这样使用
	//jd.SEQ = <-job.ch
	return outid
}

func (ts *TimeoutSchedule) AddCheckJobWithTime(t time.Time, cb func(jc JobContext) bool) int64 {
	return ts.AddCheckJobWithTimeV2(t, cb, "")
}

func (ts *TimeoutSchedule) updateJobStatus(job *Job, status JobStatue) {
	job.Status = status
	ts.updateCh <- job
}

func (ts *TimeoutSchedule) AddCheckJob(d time.Duration, cb func(jc JobContext) bool) int64 {
	jd := JobID{
		ID: time.Now().Add(d).UnixNano() / 1e6,
		// 冲突项
		SEQ: 0,
	}
	outid := int64(ts.Generate())
	job := &Job{JobID: jd,
		cb:    cb,
		OutID: outid,
		data:  map[string]interface{}{},
	}
	ts.updateJobStatus(job, WAITING)
	ts.addCh <- job
	// JobID maybe modify
	// chan 是引用传递方式，所以可以这样使用
	//jd.SEQ = <-job.ch
	return outid
}

func (ts *TimeoutSchedule) RemoveCheckJob(jid int64) {
	ts.removeCh <- struct {
		oid    int64
		ignore bool
	}{jid, false}
}

type jobDone struct {
	*Job
	result bool
}

func (ts *TimeoutSchedule) process(clock *time.Timer, doWorks chan jobDone) {
	now := time.Now().UnixNano() / 1e6
	var dropItems []interface{}
	it := ts.Iterator()
	for it.Next() {
		if job, ok := it.Value().(*Job); ok {
			if t := job.ID - now; t <= 0 {
				if job.cb == nil {
					job.cb = func(JobContext) bool { return true }
				}
				go func() {
					// 控制并发度，并处理结果, false 的要重新reschedule
					ts.updateJobStatus(job, RUNING)
					doWorks <- jobDone{job, job.cb(job)}
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
func (ts *TimeoutSchedule) Start() {
	clock := time.NewTimer(365 * 24 * time.Hour)
	doWorks := make(chan jobDone, 1024)
	go ts.updateJob()
	go func() {
		for {
			select {
			case jd := <-doWorks:
				if !jd.result {
					//fmt.Println("*************", jd.Job.Retry, jd.Job.MaxRetry, jd.Job.Retry <= jd.Job.MaxRetry)
					d := jd.Job.nextTime.Sub(time.Now())
					// retry
					if d > 0 {
						jd.Job.JobID.ID = jd.Job.nextTime.UnixNano() / 1e6
						jd.Job.runCount += 1

						ts.updateJobStatus(jd.Job, WAITING)

						ts.addCh <- jd.Job
					} else {
						ts.updateJobStatus(jd.Job, DONE)
						ts.removeCh <- struct {
							oid    int64
							ignore bool
						}{jd.Job.OutID, true}
					}

				} else {
					//fmt.Println(time.Now(), jd.Job.OutID)
					ts.updateJobStatus(jd.Job, DONE)
					ts.removeCh <- struct {
						oid    int64
						ignore bool
					}{jd.Job.OutID, true}
				}
			}
		}
	}()
	for {
		select {
		case outID := <-ts.removeCh:
			if outID.ignore {
				delete(ts.index, outID.oid)
			} else {
				if job, found := ts.index[outID.oid]; found {
					if _, ok := ts.Delete(job.JobID); ok {
					}
					delete(ts.index, outID.oid)
				}
			}
		case job := <-ts.addCh:
			if job.runCount > 0 {
				// 说明已经被主动删除了
				if _, found := ts.index[job.OutID]; !found {
					break
				}
			}
			it := ts.Seek(job.JobID)
			if it == nil {
				ts.Set(job.JobID, job)
			} else {
				preJob := it.Value().(*Job)
				seq := 0
				for preJob.ID == job.ID {
					seq = preJob.SEQ
					it.Next()
					preJob = it.Value().(*Job)
				}
				seq += 1
				job.SEQ = seq
				ts.Set(job.JobID, job)
			}
			ts.index[job.OutID] = job
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
