package halo

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-netty/go-netty"
	"github.com/go-netty/go-netty/codec/xhttp"
)

type HttpServer struct {
	consortor Consortor
	port      string
	initFunc  func(channel netty.Channel)
	ctx       context.Context
}

func NewHttpServer(ctx context.Context, port int, consortor Consortor) *HttpServer {
	server := &HttpServer{
		consortor: consortor,
		port:      strconv.Itoa(port),
		ctx:       ctx,
	}
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/addjob", server.handlerAddjob)
	httpMux.HandleFunc("/listjob", server.handlerListjob)
	httpMux.HandleFunc("/deletejob", server.handlerDeletejob)
	server.initFunc = func(channel netty.Channel) {
		channel.Pipeline().
			// decode http request from channel
			AddLast(xhttp.ServerCodec()).
			// print http access log
			AddLast(server).
			// compatible with http.Handler
			AddLast(xhttp.Handler(httpMux))
	}
	return server
}

func (h *HttpServer) handlerAddjob(writer http.ResponseWriter, request *http.Request) {
	content, _ := io.ReadAll(request.Body)
	jobContext := new(JobContext)
	if err := json.Unmarshal(content, jobContext); err == nil {
		jobid := h.consortor.addJob(jobContext)
		message := "Sccuess"
		if jobid == "" {
			message = "Fail"
		}
		reuslt := &ResultData[string]{
			Message: message,
			Data:    jobid,
		}
		data, _ := json.Marshal(reuslt)
		writer.Write(data)
	} else {
		writer.Write([]byte(err.Error()))
	}
}
func (h *HttpServer) handlerListjob(writer http.ResponseWriter, request *http.Request) {
	data, _ := json.Marshal(h.consortor.listJob())
	writer.Write(data)
}

func (h *HttpServer) handlerDeletejob(writer http.ResponseWriter, request *http.Request) {
	jobid := request.URL.Query().Get("jobid")
	if err := h.consortor.deleteJob(jobid); err != nil {
		writer.Write([]byte(err.Error()))
	} else {
		reuslt := &ResultData[string]{
			Message: "Sccuess",
			Data:    jobid,
		}
		data, _ := json.Marshal(reuslt)
		writer.Write(data)
	}
}

func (h *HttpServer) HandleActive(ctx netty.ActiveContext) {
	ctx.HandleActive()
}

func (h *HttpServer) HandleRead(ctx netty.InboundContext, message netty.Message) {
	if request, ok := message.(*http.Request); ok {
		fmt.Printf("[%d]%s: %s %s\n", ctx.Channel().ID(), ctx.Channel().RemoteAddr(), request.Method, request.URL.Path)
	}
	ctx.HandleRead(message)
}

func (h *HttpServer) HandleWrite(ctx netty.OutboundContext, message netty.Message) {
	if responseWriter, ok := message.(http.ResponseWriter); ok {
		responseWriter.Header().Add("x-time", time.Now().String())
	}
	ctx.HandleWrite(message)
}

func (h *HttpServer) HandleInactive(ctx netty.InactiveContext, ex netty.Exception) {
	ctx.HandleInactive(ex)
}
