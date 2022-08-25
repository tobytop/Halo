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
)

type Client struct {
	addr          string
	retryCount    int
	retryInterval int64
	initFunc      func(channel netty.Channel)
	handlers      map[string]JobHandler
	ctx           context.Context
	bootstrap     netty.Bootstrap
	conn          netty.Channel
}

func NewClient(ctx context.Context, addr string, handlers func() map[string]JobHandler) *Client {
	client := &Client{
		addr:          addr,
		retryInterval: 120,
		retryCount:    3,
		handlers:      handlers(),
		ctx:           ctx,
	}
	client.initFunc = func(channel netty.Channel) {
		channel.Pipeline().
			AddLast(frame.LengthFieldCodec(binary.LittleEndian, 1024, 0, 2, 0, 2)).
			AddLast(format.TextCodec()).
			AddLast(client)
	}
	return client
}

func (c *Client) StartServer() {
	c.bootstrap = netty.NewBootstrap(netty.WithChildInitializer(c.initFunc))
	c.conn, _ = c.bootstrap.Connect(c.addr, nil)
}

func (c *Client) HandleActive(ctx netty.ActiveContext) {
	handlers := []string{}
	for key, _ := range c.handlers {
		handlers = append(handlers, key)
	}
	msg := &SendMsg[SendData]{
		Option: Msg_Server,
		Data: SendData{
			Types: handlers,
		},
	}

	data, _ := json.Marshal(msg)
	ctx.Write(data)
	ctx.HandleActive()
}

func (c *Client) HandleRead(ctx netty.InboundContext, message netty.Message) {
	msg := message.(string)
	data := []byte(msg)
	sendMsg := new(SendMsg[SendData])
	if err := json.Unmarshal(data, sendMsg); err == nil {
		switch sendMsg.Option {
		case Msg_Job:
			ants.Submit(func() {
				if _, ok := c.handlers[sendMsg.Data.Handler]; ok {
					sendMsg.Option = Msg_Hunting
					data, _ := json.Marshal(sendMsg)
					ctx.Write(data)
					ctx.HandleRead(message)
				}
			})
		case Msg_Get:
			ants.Submit(func() {
				c.startNewJob(data)
			})
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

func (c *Client) startNewJob(msg []byte) {
	jobInfo := JobContext{}
	if errjson := json.Unmarshal(msg, &jobInfo); errjson != nil {
		log.Println("ERROR", errjson)
	}
	var job Worker

	if handler, ok := c.handlers[jobInfo.Type]; ok {
		if jobInfo.Cron != "" {
			job = NewCronJob(c.ctx, jobInfo)
		} else {
			job = NewSimpleWorker(c.ctx, jobInfo)
		}
		job.StartWorker(handler)
	} else {
		log.Println("ERROR", "the handler not found")
	}
}

func (c *Client) StopServer() {
	if c.conn != nil {
		sendMsg := &SendMsg[SendData]{
			Option: Msg_Stop,
		}
		data, _ := json.Marshal(sendMsg)
		c.conn.Write(data)
		c.bootstrap.Shutdown()
	}
}
