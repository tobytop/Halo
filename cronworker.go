package halo

import (
	"context"
)

type CronWorker struct {
	Id          string
	Name        string
	Group       string
	Description string
	Cron        string
	ErrorMode   int
	JobData     map[string]string
	ctx         context.Context
	client      *Client
}

func NewCronJob(ctx context.Context, job JobContext, client *Client) *CronWorker {
	return &CronWorker{
		Id:          job.Id,
		Name:        job.Name,
		Group:       job.Group,
		Description: job.Description,
		Cron:        job.Cron,
		ErrorMode:   job.ErrorMode,
		JobData:     job.JobData,
		client:      client,
	}
}

func (worker *CronWorker) StartWorker(handler JobHandler) {
	worker.client.cron.AddFunc(worker.Cron, func() {
		if err := handler.Execute(worker.ctx, worker); err != nil {
			panic(err)
		}
	})
}
