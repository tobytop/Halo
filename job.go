package halo

import "context"

type JobContext struct {
	Id            string            `json:"id"`
	Name          string            `json:"name"`
	Group         string            `json:"group"`
	Description   string            `json:"description"`
	Type          string            `json:"method"`
	StartTime     int64             `json:"starttime"`
	EndTime       int64             `json:"endtime"`
	Cron          string            `json:"cron"`
	ErrorMode     int               `json:"errormode"`
	RetryCount    int               `json:"retrycount"`
	RetryInterval int               `json:"retryinterval"`
	JobData       map[string]string `json:"jobdata"`
	Status        int               `json:"status"`
}

type JobHandler interface {
	Execute(ctx context.Context, worker interface{}) error
}

type SendMsg[T interface{}] struct {
	Option string `json:"option"`
	Data   T      `json:"data"`
}

type SendData struct {
	JobId   string `json:"jobid"`
	Handler string `json:"handler"`
}

type ResultData[T interface{}] struct {
	Message string `json:"message"`
	Data    T      `json:"data"`
}

var (
	Msg_Server    = "!Msg_Server"
	Msg_Job       = "!Msg_job"
	Msg_JobStatus = "!Msg_JobStatus"
	Msg_Hunting   = "!Msg_Hunting"
	Msg_Get       = "!Msg_get"
	Msg_Stop      = "!Msg_Stop"
)
