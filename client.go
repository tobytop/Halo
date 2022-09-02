package halo

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
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
	weight        int
	retryInterval int64
	initFunc      func(channel netty.Channel)
	handlers      map[string]JobHandler
	jobs          map[string]interface{}
	ctx           context.Context
	bootstrap     netty.Bootstrap
	conn          netty.Channel
	action        chan string
	cron          *cron.Cron
	cancel        context.CancelFunc
}

func NewDefaultClient(addr string, handlers func() map[string]JobHandler) *Client {
	return NewClient(context.Background(), WtihDefaultOpts(addr, handlers()))
}

func NewClient(ctx context.Context, options ...Option) *Client {
	opts := loadOptions(options...)
	fmt.Println("halo:", "->", "opt:", opts)
	cron := cron.New(cron.WithChain(
		func() cron.JobWrapper {
			switch opts.CronMode {
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
		addr:          opts.Addr,
		retryInterval: opts.RetryInterval,
		retryCount:    opts.RetryCount,
		jobs:          make(map[string]interface{}),
		handlers:      opts.Handlers,
		weight:        opts.Weight,
		ctx:           ctx,
		action:        make(chan string),
		cron:          cron,
		cancel:        cancel,
	}
	client.initFunc = func(channel netty.Channel) {
		channel.Pipeline().
			AddLast(frame.LengthFieldCodec(binary.LittleEndian, 1024*10, 0, 2, 0, 2)).
			AddLast(format.TextCodec()).
			AddLast(client)
	}
	return client
}

func (c *Client) reaction() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case data := <-c.action:
			ants.Submit(func() {
				delete(c.jobs, data)
				msg := &SendMsg[string]{
					Option: Msg_JobStatus,
					Data:   data,
				}
				data, _ := json.Marshal(msg)
				c.conn.Write(string(data))
			})
		}
	}
}

func (c *Client) StartServer() {
	ants.Submit(func() {
		c.reaction()
	})
	c.cron.Start()
	c.bootstrap = netty.NewBootstrap(netty.WithClientInitializer(c.initFunc))
	c.conn, _ = c.bootstrap.Connect(c.addr, nil)
	in := bufio.NewReader(os.Stdin)
	out := bufio.NewWriter(os.Stdout)
	fmt.Fprintln(out, "The connection has been started and you can exit via command exit")
	out.Flush()
	defer c.StopServer()
	for {
		input, _ := in.ReadString('\n')
		if input[:len(input)-2] == "exit" {
			c.StopServer()
			os.Exit(0)
		}
	}
}

func (c *Client) HandleActive(ctx netty.ActiveContext) {
	handlers := []string{}
	for key := range c.handlers {
		handlers = append(handlers, key)
	}
	msg := &SendMsg[[]string]{
		Option: Msg_Server,
		Data:   handlers,
	}

	data, _ := json.Marshal(msg)
	ctx.Write(string(data))
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
			if _, ok := c.handlers[msgData.Handler]; ok {
				sendMsg.Option = Msg_Hunting
				data, _ := json.Marshal(sendMsg)
				ctx.Write(string(data))
				ctx.HandleRead(message)
			}
		case Msg_Get:
			msgData := sendMsg.Data.(JobContext)
			c.startNewJob(msgData)
		case Msg_Delete:
			msgData := sendMsg.Data.(string)
			switch worker := c.jobs[msgData].(type) {
			case CronWorker:
				c.cron.Remove(worker.cronId)
			case SimpleWorker:
				worker.stop <- 1
			}
			delete(c.jobs, msgData)
		}
	}
}

func (c *Client) HandleInactive(ctx netty.InactiveContext, ex netty.Exception) {
	c.reconnect(c.retryCount + 1)
	ctx.HandleInactive(ex)
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
	c.cron.Stop()
	if c.conn != nil {
		sendMsg := &SendMsg[string]{
			Option: Msg_Stop,
		}
		data, _ := json.Marshal(sendMsg)
		c.conn.Write(string(data))
		c.bootstrap.Shutdown()
	}
	c.cancel()
}
