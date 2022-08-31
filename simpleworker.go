package halo

import (
	"context"
	"time"
)

type Worker interface {
	StartWorker(handler JobHandler)
}

type SimpleWorker struct {
	Id            string
	Name          string
	Group         string
	Description   string
	StartTime     int64
	EndTime       int64
	RetryCount    int
	RetryInterval int
	JobData       map[string]string
	ctx           context.Context
	client        *Client
	stop          chan byte
}

func NewSimpleWorker(ctx context.Context, job JobContext, client *Client) *SimpleWorker {
	return &SimpleWorker{
		Id:            job.Id,
		Name:          job.Name,
		Group:         job.Group,
		Description:   job.Description,
		StartTime:     job.StartTime,
		EndTime:       job.EndTime,
		RetryCount:    job.RetryCount,
		RetryInterval: job.RetryInterval,
		JobData:       job.JobData,
		ctx:           ctx,
		client:        client,
		stop:          make(chan byte, 1),
	}
}

func (worker *SimpleWorker) StartWorker(handler JobHandler) {
	var beginConter *time.Timer
	if timeSpan := time.Now().Unix() - worker.StartTime; timeSpan > 0 {
		beginConter = time.NewTimer(time.Duration(timeSpan) * time.Second)
	} else {
		beginConter = time.NewTimer(0)
	}
	defer beginConter.Stop()
	select {
	case <-worker.stop:
		return
	case <-worker.ctx.Done():
		return
	case <-beginConter.C:
		if err := handler.Execute(worker.ctx, worker); err != nil {
			if worker.RetryCount > 0 {
				worker.StartTime = time.Now().Add(time.Duration(worker.RetryInterval) * time.Second).Unix()
				worker.RetryCount--
				worker.StartWorker(handler)
			}
		} else {
			worker.client.action <- worker.Id
		}
	}
}
