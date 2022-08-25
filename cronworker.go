package halo

import (
	"context"

	"github.com/robfig/cron/v3"
)

type CronWorker struct {
	Id          string
	Name        string
	Group       string
	Description string
	Cron        string
	ErrorMode   int
	JobData     map[string]string
	c           *cron.Cron
	ctx         context.Context
}

func NewCronJob(ctx context.Context, job JobContext) *CronWorker {
	return &CronWorker{
		Id:          job.Id,
		Name:        job.Name,
		Group:       job.Group,
		Description: job.Description,
		Cron:        job.Cron,
		ErrorMode:   job.ErrorMode,
		JobData:     job.JobData,
	}
}

func (worker *CronWorker) StartWorker(handler JobHandler) {
	cron := cron.New(cron.WithChain(
		func() cron.JobWrapper {
			switch worker.ErrorMode {
			case 0:
				return cron.Recover(cron.DefaultLogger)
			case 1:
				return cron.DelayIfStillRunning(cron.DefaultLogger)
			default:
				return cron.SkipIfStillRunning(cron.DefaultLogger)
			}
		}(),
	))
	worker.c = cron
	cron.AddFunc(worker.Cron, func() {
		if err := handler.Execute(worker.ctx, worker); err != nil {
			panic(err)
		}
	})
	cron.Start()
}

func (worker *CronWorker) StopWorker(cancel context.CancelFunc) error {
	<-worker.c.Stop().Done()
	return nil
}
