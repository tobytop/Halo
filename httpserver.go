package halo

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
)

type HttpServer struct {
	consortor Consortor
	port      string
	router    *http.ServeMux
	server    *Server
}

func NewHttpServer(port int, consortor Consortor, tcpserver *Server) *HttpServer {
	server := &HttpServer{
		consortor: consortor,
		port:      strconv.Itoa(port),
		server:    tcpserver,
	}
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/gettaskhandler", server.handlerAllTaskHandler)
	httpMux.HandleFunc("/addjob", server.handlerAddjob)
	httpMux.HandleFunc("/listjob", server.handlerListjob)
	httpMux.HandleFunc("/deletejob", server.handlerDeletejob)
	server.router = httpMux
	return server
}

func (h *HttpServer) StartServer() {
	http.ListenAndServe(":"+h.port, h.router)
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
	msg := &controlMsg{
		msgType: deleteJob,
		jobId:   jobid,
		addr:    h.consortor.findAddrByjobId(jobid),
	}
	h.server.controlMsg <- msg
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

func (h *HttpServer) handlerAllTaskHandler(writer http.ResponseWriter, request *http.Request) {
	handlers := make(map[string]bool)
	for _, client := range h.server.connects {
		if client.status == ACTIVE {
			for _, handler := range client.Types {
				handlers[handler] = true
			}
		}
	}
	allhandlers := []string{}
	for key := range handlers {
		allhandlers = append(allhandlers, key)
	}
	reuslt := &ResultData[[]string]{
		Message: "Sccuess",
		Data:    allhandlers,
	}
	data, _ := json.Marshal(reuslt)
	writer.Write(data)
}
