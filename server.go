package halo

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-netty/go-netty"
	"github.com/go-netty/go-netty/codec/format"
	"github.com/go-netty/go-netty/codec/frame"
	"github.com/panjf2000/ants/v2"
	"golang.org/x/exp/slices"
)

type Server struct {
	consortor     Consortor
	port          string
	balanceMode   int
	controlMsg    chan *controlMsg
	connects      map[string]*serverInfo
	ctx           context.Context
	initFunc      func(channel netty.Channel)
	election      election
	retryCount    int
	retryInterval int64
	mu            sync.Mutex
}

type controlMsg struct {
	job     JobContext
	msgType string
	addr    string
	jobId   string
}

const (
	lostClient = "LostClient"
	sendJob    = "SendJob"
	publishJob = "PublishJob"
	deleteJob  = "DeleteJob"
)

type serverInfo struct {
	channel netty.Channel
	Types   []string
	status  int
	weight  int
	addr    string
}

const (
	ACTIVE = iota
	PENDING
	STOP
)

func NewServer(ctx context.Context, port int, balanceMode int, consortor Consortor) *Server {
	server := &Server{
		port:          fmt.Sprintf(":%d", port),
		consortor:     consortor,
		connects:      make(map[string]*serverInfo),
		controlMsg:    make(chan *controlMsg),
		ctx:           ctx,
		balanceMode:   balanceMode,
		retryInterval: 120,
		retryCount:    3,
	}
	switch balanceMode {
	case 1:
		server.election = &roundRobinBalance{
			curIndex: 0,
		}
	case 2:
		server.election = &weightRoundRobinBalance{}
	}

	server.initFunc = func(channel netty.Channel) {
		channel.Pipeline().
			AddLast(frame.LengthFieldCodec(binary.LittleEndian, 1024*10, 0, 2, 0, 2)).
			AddLast(format.TextCodec()).
			AddLast(server)
	}

	consortor.setServer(server)
	return server
}

func (center *Server) HandleActive(ctx netty.ActiveContext) {
	ctx.HandleActive()
}

func (center *Server) HandleRead(ctx netty.InboundContext, message netty.Message) {
	msg := message.(string)
	sendMsg := new(SendMsg[interface{}])
	if err := json.Unmarshal([]byte(msg), sendMsg); err != nil {
		fmt.Println("halo:", "->", "readerr:", ctx.Channel().RemoteAddr(), err)
		return
	}
	addr := ctx.Channel().RemoteAddr()
	fmt.Println("halo:", "->", "message:", ctx.Channel().RemoteAddr(), sendMsg)
	switch sendMsg.Option {
	case Msg_Server:
		data := sendMsg.Data.(map[string]interface{})
		var (
			handlers []string
			weight   int
		)
		for key, value := range data {
			switch key {
			case "weight":
				weight = int(value.(float64))
			case "Handlers":
				serverHandlers := value.([]interface{})
				handlers := make([]string, len(serverHandlers))
				for i, value := range serverHandlers {
					handlers[i] = value.(string)
				}
			}
		}
		center.mu.Lock()
		defer center.mu.Unlock()
		if value, ok := center.connects[addr]; ok {
			value.status = ACTIVE
			value.Types = handlers
		} else {
			center.connects[addr] = &serverInfo{
				channel: ctx.Channel(),
				status:  ACTIVE,
				Types:   handlers,
				weight:  weight,
				addr:    addr,
			}
		}
		center.election.add(center.connects[addr])
	case Msg_JobStatus:
		data := sendMsg.Data.(string)
		center.consortor.finishJob(data)
	case Msg_Stop:
		center.mu.Lock()
		defer center.mu.Unlock()
		center.connects[addr].status = STOP
	}
	ctx.HandleRead(message)
}

func (center *Server) HandleInactive(ctx netty.InactiveContext, ex netty.Exception) {
	addr := ctx.Channel().RemoteAddr()
	if center.connects[addr].status == STOP {
		return
	}

	msg := &controlMsg{
		msgType: lostClient,
		addr:    addr,
	}
	center.mu.Lock()
	defer center.mu.Unlock()
	center.connects[addr].status = PENDING
	center.controlMsg <- msg
	fmt.Println("halo:", "->", "inactive:", ctx.Channel().RemoteAddr(), ex)
	ctx.HandleInactive(ex)
}

func (center *Server) HandleException(ctx netty.ExceptionContext, ex netty.Exception) {
	fmt.Println("halo:", "->", "tcperr:", ctx.Channel().RemoteAddr(), ex)
}

func (center *Server) StartServer() *Server {
	ants.Submit(func() {
		center.reaction()
	})

	if err := netty.NewBootstrap(netty.WithChildInitializer(center.initFunc)).Listen(center.port).Sync(); err != nil {
		panic(err)
	}
	return center
}

func (center *Server) BuilderHttpService(port int) *HttpServer {
	return NewHttpServer(port, center)
}

func (center *Server) reaction() {
	for {
		select {
		case <-center.ctx.Done():
			return
		case msg := <-center.controlMsg:
			switch msg.msgType {
			case sendJob:
				ants.Submit(func() {
					sendmsg := &SendMsg[JobContext]{
						Option: Msg_Get,
						Data:   msg.job,
					}
					if jsonString, err := json.Marshal(sendmsg); err == nil {
						center.connects[msg.addr].channel.Write(string(jsonString))
					} else {
						fmt.Println("halo:", "->", "reaction:", sendJob, err)
					}
				})
			case publishJob:
				ants.Submit(func() {
					center.publishJob(msg.job.Type, msg.jobId)
				})
			case lostClient:
				ants.Submit(func() {
					center.deleteServer(msg.addr, center.retryCount+1)
				})
			case deleteJob:
				ants.Submit(func() {
					sendmsg := &SendMsg[string]{
						Option: Msg_Get,
						Data:   msg.jobId,
					}
					if jsonString, err := json.Marshal(sendmsg); err == nil {
						center.connects[msg.addr].channel.Write(string(jsonString))
					} else {
						fmt.Println("halo:", "->", "reaction:", deleteJob, err)
					}
				})
			}
		}
	}
}
func (center *Server) deleteServer(addr string, count int) {
	beginConter := time.NewTimer(time.Duration(center.retryInterval*int64(count-center.retryCount)) * time.Second)
	defer beginConter.Stop()
	<-beginConter.C
	if value, ok := center.connects[addr]; ok && value.status != PENDING {
		return
	}
	center.retryCount--
	if center.retryCount > 0 {
		center.deleteServer(addr, count)
	} else {
		center.mu.Lock()
		defer center.mu.Unlock()
		delete(center.connects, addr)
		center.consortor.removeServerJob(addr)
		center.election.remove(center.connects[addr])
	}
}
func (center *Server) publishJob(handler, jobId string) {
	if center.getRunningServerNum() == 0 {
		return
	}
	addr := ""
	for {
		tempaddr := center.election.next()
		for _, client := range center.connects {
			if slices.Contains(client.Types, handler) && tempaddr == client.addr {
				if client.status == RUNING {
					addr = tempaddr
					center.election.setWegiht(1, addr)
					break
				} else {
					center.election.setWegiht(-1, addr)
				}
				break
			}
		}
		if addr != "" {
			break
		}
	}
	job := center.consortor.huntingJob(addr, jobId)
	if job != nil {
		msg := &controlMsg{
			job:     *job,
			msgType: sendJob,
			addr:    addr,
		}
		center.controlMsg <- msg
	}
}

func (center *Server) getRunningServerNum() int {
	addrList := []string{}
	for _, client := range center.connects {
		if client.status == RUNING {
			addrList = append(addrList, client.addr)
		}
	}
	return len(addrList)
}
