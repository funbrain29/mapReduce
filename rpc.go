package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

// Nothing ()
type Nothing struct{}

// Response ()
type Response struct {
	Message    string
	Maptask    MapTask
	Reducetask ReduceTask
	WorkType   int // 0 for no work, 1 for mapping, 2 for reducing
	Shutdown   bool
}

type LocalResponse struct {
	TasksDone   bool
	AddressList []string
}

// Node (FingerTable, Successor, Predecessor, Bucket)
type Master struct {
	MapTasks    []MapTask
	ReduceTasks []ReduceTask
	Mappers     []string
	MapProg     []int
	Reducers    []string
	ReduceProg  []int
	Shutdown    bool
}

type handler func(*Master)

// Server (chan handler)
type Server chan<- handler

// Ping (key, value, reply)
func (s Server) Ping(ip string, reply *Response) error {
	finished := make(chan struct{})
	s <- func(f *Master) {
		log.Printf("Recieved Ping message, responding...\n")
		reply.Message = "Ping Successful"
		finished <- struct{}{}
	}
	<-finished
	return nil
}

// Ping (key, value, reply)
func (s Server) GetWork(ip string, reply *Response) error {
	finished := make(chan struct{})
	s <- func(f *Master) {
		// check to see if shutting down
		if f.Shutdown {
			reply.WorkType = 0
			reply.Shutdown = true
			finished <- struct{}{}
			return
		}
		// check for map work
		for i := range f.MapTasks {
			if f.MapProg[i] == 0 { // there is work availaible
				fmt.Printf("Worker '%s' has taken map job #%v\n", ip, i)
				f.MapProg[i] = 1
				f.Mappers[i] = ip
				reply.Maptask = f.MapTasks[i]
				reply.WorkType = 1
				reply.Shutdown = false
				finished <- struct{}{}
				return
			}
		}
		// check for reduce work
		for i := range f.ReduceTasks {
			if f.ReduceProg[i] == 0 { // there is work availaible
				fmt.Printf("Worker '%s' has taken reduce job #%v\n", ip, i)
				f.ReduceProg[i] = 1
				f.Reducers[i] = ip
				reply.Reducetask = f.ReduceTasks[i]
				reply.WorkType = 2
				reply.Shutdown = false
				finished <- struct{}{}
				return
			}
		}
		// there is no work available
		reply.WorkType = 0
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) FinishedWork(ip string, reply *Response) error {
	finished := make(chan struct{})
	s <- func(f *Master) {
		for i := range f.MapTasks {
			if f.Mappers[i] == ip {
				f.MapProg[i] = 2
			}
		}
		for i := range f.ReduceTasks {
			if f.Reducers[i] == ip {
				f.ReduceProg[i] = 2
			}
		}
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) ExecuteMapTasks(Tasks []MapTask, junk *Nothing) error {
	finished := make(chan struct{})
	s <- func(f *Master) {
		f.MapTasks = Tasks
		for range f.MapTasks {
			f.MapProg = append(f.MapProg, 0)
			f.Mappers = append(f.Mappers, "")
		}
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) GetMapTaskFinished(junk Nothing, response *LocalResponse) error {
	finished := make(chan struct{})
	s <- func(f *Master) {
		done := true
		for i := range f.MapTasks {
			if f.MapProg[i] != 2 {
				done = false
			}
		}
		// if all MapProg values are "2"
		if done {
			response.TasksDone = true
			response.AddressList = f.Mappers
			// cleanup map tasks
			f.MapTasks = f.MapTasks[:0]
			f.MapProg = f.MapProg[:0]
			f.Mappers = f.Mappers[:0]
		} else {
			response.TasksDone = false
		}
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) ExecuteReduceTasks(Tasks []ReduceTask, junk *Nothing) error {
	finished := make(chan struct{})
	s <- func(f *Master) {
		f.ReduceTasks = Tasks
		for range f.ReduceTasks {
			f.ReduceProg = append(f.ReduceProg, 0)
			f.Reducers = append(f.Reducers, "")
		}
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) GetReduceTaskFinished(junk Nothing, response *LocalResponse) error {
	finished := make(chan struct{})
	s <- func(f *Master) {
		done := true
		for i := range f.ReduceTasks {
			if f.ReduceProg[i] != 2 {
				done = false
			}
		}
		// if all MapProg values are "2"
		if done {
			response.TasksDone = true
			response.AddressList = f.Reducers
			// cleanup map tasks
			f.ReduceTasks = f.ReduceTasks[:0]
			f.ReduceProg = f.ReduceProg[:0]
			f.Reducers = f.Reducers[:0]
		} else {
			response.TasksDone = false
		}
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) Shutdown(_ Nothing, _ *Nothing) error {
	finished := make(chan struct{})
	s <- func(f *Master) {
		f.Shutdown = true
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func startMActor() Server {
	ch := make(chan handler)
	state := new(Master)
	go func() {
		for f := range ch {
			f(state)
		}
	}()
	return ch
}

func masterServer(address string, port int, tempdir string) Server {
	actor := startMActor()
	rpc.Register(actor)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
	go http.Serve(l, nil)

	fmt.Printf("RPC and File Server Running on %v\n", address)
	return actor
}

func getLocalAddress(port int) string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String() + ":" + fmt.Sprint(port)
}

func callErr(address string, method string, request interface{}, response interface{}) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return err
	}
	defer client.Close()

	if err = client.Call(method, request, response); err != nil {
		return err
	}
	return nil
}
