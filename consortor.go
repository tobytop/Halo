package halo

import (
	"encoding/json"
	"strconv"
	"sync"

	"github.com/google/uuid"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-cleanhttp"
)

type Consortor interface {
	setServer(server *Server)
	addJob(job *JobContext) string
	finishJob(jobid string)
	huntingJob(addr, jobid string) *JobContext
	removeServerJob(addr string)
	listJob() []JobContext
	deleteJob(jobid string) error
	findAddrByjobId(jobid string) string
	detailJob(jobid string) *JobContext
}

const (
	JOBSERVER_PREFIX = "jobserver"
	JOB_PREFIX       = "job"
	JOBSTATUS_PREFIX = "jobstatus"
	LOCK_KEY         = "joblock"
)

const (
	WAITING = iota
	RUNING
	ERROR
	FINISH
)

type DefaultConsortor struct {
	client *api.Client
	server *Server
	kv     *api.KV
	mu     sync.Mutex
}

func NewConsortor(host string) (*DefaultConsortor, error) {
	config := &api.Config{
		Address:   host,
		Scheme:    "http",
		Transport: cleanhttp.DefaultPooledTransport(),
	}
	client, err := api.NewClient(config)
	return &DefaultConsortor{
		client: client,
		kv:     client.KV(),
	}, err
}

func (consortor *DefaultConsortor) setServer(server *Server) {
	consortor.server = server
}

func (consortor *DefaultConsortor) addJob(job *JobContext) string {
	consortor.mu.Lock()
	defer consortor.mu.Unlock()
	jobid := uuid.New().String()
	if data, err := json.Marshal(job); err == nil {
		consortor.kv.Put(&api.KVPair{
			Key:   JOB_PREFIX + "/" + jobid,
			Value: data,
		}, nil)
		consortor.kv.Put(&api.KVPair{
			Key:   JOBSTATUS_PREFIX + "/" + jobid,
			Value: []byte(strconv.Itoa(WAITING)),
		}, nil)
		consortor.server.controlMsg <- &controlMsg{
			jobId:   jobid,
			job:     *job,
			msgType: publishJob,
		}
		return jobid
	} else {
		return ""
	}
}

func (consortor *DefaultConsortor) huntingJob(addr, jobid string) *JobContext {
	p, _, _ := consortor.kv.Get(JOBSTATUS_PREFIX+"/"+jobid, nil)
	status, _ := strconv.Atoi(string(p.Value))
	if status == WAITING || status == ERROR {
		jobkey := JOBSERVER_PREFIX + "/" + jobid
		keys, _, _ := consortor.kv.Keys(JOBSERVER_PREFIX, "/", nil)
		for _, v := range keys {
			if v == jobkey {
				return nil
			}
		}
		consortor.kv.Put(&api.KVPair{
			Key:   jobkey,
			Value: []byte(addr),
		}, nil)

		return consortor.detailJob(jobid)
	}

	return nil
}

func (consortor *DefaultConsortor) detailJob(jobid string) *JobContext {
	pair, _, _ := consortor.kv.Get(JOB_PREFIX+"/"+jobid, nil)
	job := &JobContext{}
	json.Unmarshal(pair.Value, job)
	return job
}

func (consortor *DefaultConsortor) finishJob(jobid string) {
	consortor.mu.Lock()
	defer consortor.mu.Unlock()
	consortor.kv.Put(&api.KVPair{
		Key:   JOBSTATUS_PREFIX + "/" + jobid,
		Value: []byte(strconv.Itoa(FINISH)),
	}, nil)
}

func (consortor *DefaultConsortor) findAddrByjobId(jobid string) string {
	jobkey := JOBSERVER_PREFIX + "/" + jobid
	pair, _, _ := consortor.kv.Get(jobkey, nil)
	return string(pair.Value)
}

func (consortor *DefaultConsortor) listJob() []JobContext {
	pairs, _, _ := consortor.kv.List(JOB_PREFIX, nil)
	jobs := []JobContext{}
	for _, p := range pairs {
		job := &JobContext{}
		json.Unmarshal(p.Value, job)
		jobs = append(jobs, *job)
	}
	return jobs
}

func (consortor *DefaultConsortor) deleteJob(jobid string) error {
	_, err := consortor.kv.Delete(JOBSTATUS_PREFIX+"/"+jobid, nil)
	return err
}

func (consortor *DefaultConsortor) removeServerJob(addr string) {
	consortor.mu.Lock()
	defer consortor.mu.Unlock()
	pairs, _, _ := consortor.kv.List(JOBSERVER_PREFIX, nil)
	for _, p := range pairs {
		data := string(p.Value)
		if data == addr {
			jobid := p.Key[len(JOBSERVER_PREFIX):]
			pv, _, _ := consortor.kv.Get(JOBSTATUS_PREFIX+jobid, nil)
			status, _ := strconv.Atoi(string(pv.Value))
			if status == RUNING {
				pv.Value = []byte(strconv.Itoa(ERROR))
				consortor.kv.Put(pv, nil)
			}
		}
	}
}
