package halo

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"log"
	"time"

	"github.com/go-netty/go-netty"
	"github.com/go-netty/go-netty/codec/format"
	"github.com/go-netty/go-netty/codec/frame"
	"github.com/panjf2000/ants/v2"
	"github.com/robfig/cron/v3"
)

type Client struct {
	addr          string
	retryCount    int
	retryInterval int64
	initFunc      func(channel netty.Channel)
	handlers      map[string]JobHandler
	jobs          map[string]interface{}
	ctx           context.Context
	bootstrap     netty.Bootstrap
	conn          netty.Channel
	action        chan string
	stop          chan byte
	cron          *cron.Cron
	cancel        context.CancelFunc
}

func NewClient(ctx context.Context, addr string, handlers func() map[string]JobHandler, cronMode int) *Client {
	cron := cron.New(cron.WithChain(
		func() cron.JobWrapper {
			switch cronMode {
			case 0:
				return cron.Recover(cron.DefaultLogger)
			case 1:
				return cron.DelayIfStillRunning(cron.DefaultLogger)
			default:
				return cron.SkipIfStillRunning(cron.DefaultLogger)
			}
		}(),
	))
	ctx, cancel := context.WithCancel(ctx)
	client := &Client{
		addr:          addr,
		retryInterval: 120,
		retryCount:    3,
		jobs:          make(map[string]interface{}),
		handlers:      handlers(),
		ctx:           ctx,
		action:        make(chan string),
		stop:          make(chan byte),
		cron:          cron,
		cancel:        cancel,
	}
	client.initFunc = func(channel netty.Channel) {
		channel.Pipeline().
			AddLast(frame.LengthFieldCodec(binary.LittleEndian, 1024, 0, 2, 0, 2)).
			AddLast(format.TextCodec()).
			AddLast(client)
	}
	return client
}

func (c *Client) reaction() {
	for {
		select {
		case <-c.stop:
			return
		case data := <-c.action:
			ants.Submit(func() {
				delete(c.jobs, data)
				msg := &SendMsg[string]{
					Option: Msg_JobStatus,
					Data:   data,
				}
				data, _ := json.Marshal(msg)
				c.conn.Write(data)
			})
		}
	}
}

func (c *Client) StartServer() {
	ants.Submit(func() {
		c.reaction()
	})
	c.cron.Start()
	c.bootstrap = netty.NewBootstrap(netty.WithChildInitializer(c.initFunc))
	c.conn, _ = c.bootstrap.Connect(c.addr, nil)
}

func (c *Client) HandleActive(ctx netty.ActiveContext) {
	handlers := []string{}
	for key, _ := range c.handlers {
		handlers = append(handlers, key)
	}
	msg := &SendMsg[[]string]{
		Option: Msg_Server,
		Data:   handlers,
	}

	data, _ := json.Marshal(msg)
	ctx.Write(data)
	ctx.HandleActive()
}

func (c *Client) HandleRead(ctx netty.InboundContext, message netty.Message) {
	msg := message.(string)
	data := []byte(msg)
	sendMsg := new(SendMsg[interface{}])
	if err := json.Unmarshal(data, sendMsg); err == nil {
		switch sendMsg.Option {
		case Msg_Job:
			msgData := sendMsg.Data.(SendData)
			ants.Submit(func() {
				if _, ok := c.handlers[msgData.Handler]; ok {
					sendMsg.Option = Msg_Hunting
					data, _ := json.Marshal(sendMsg)
					ctx.Write(data)
					ctx.HandleRead(message)
				}
			})
		case Msg_Get:
			msgData := sendMsg.Data.(JobContext)
			ants.Submit(func() {
				c.startNewJob(msgData)
			})
		case Msg_Delete:
			msgData := sendMsg.Data.(string)
			switch cron := c.jobs[msgData].(type) {
			case CronWorker:
				c.cron.Remove(cron.cronId)
			case SimpleWorker:
				cron.stop <- 1
			}
			delete(c.jobs, msgData)
		}
	}
}

func (c *Client) HandleInactive(ctx netty.InactiveContext, ex netty.Exception) {
	c.reconnect(c.retryCount + 1)
}

func (c *Client) reconnect(count int) {
	beginConter := time.NewTimer(time.Duration(c.retryInterval*int64(count-c.retryCount)) * time.Second)
	defer beginConter.Stop()
	<-beginConter.C
	if conn, err := c.bootstrap.Connect(c.addr, nil); err == nil || c.retryCount == 0 {
		if err == nil {
			c.conn = conn
		}
		return
	} else {
		c.retryCount--
		c.reconnect(count)
	}
}

func (c *Client) startNewJob(jobInfo JobContext) {
	if handler, ok := c.handlers[jobInfo.Type]; ok {
		if jobInfo.Cron != "" {
			c.jobs[jobInfo.Id] = NewCronJob(c.ctx, jobInfo, c)
		} else {
			c.jobs[jobInfo.Id] = NewSimpleWorker(c.ctx, jobInfo, c)
		}
		c.jobs[jobInfo.Id].(Worker).StartWorker(handler)
	} else {
		log.Println("ERROR", "the handler not found")
	}
}

func (c *Client) StopServer() {
	c.cancel()
	c.cron.Stop()
	if c.conn != nil {
		sendMsg := &SendMsg[string]{
			Option: Msg_Stop,
		}
		data, _ := json.Marshal(sendMsg)
		c.conn.Write(data)
		c.bootstrap.Shutdown()
		c.stop <- 1
	}
}
