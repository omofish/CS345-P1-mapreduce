package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	viewnumber uint
	isprimary  bool
	isbackup   bool
	log		   map[string]string
	currentview viewservice.View

}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.

	//fmt.Println("in get!");

	//fmt.Println(pb.log)
	//fmt.Println(args.Key)

	if pb.isprimary {
		reply.Err = "ok"
		reply.Value = pb.log[args.Key]
		return nil
	} else {
		var Err error
		
		return Err
	}
	


	
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.

	//fmt.Println("in PutAppend!");

	//fmt.Println(pb.log)

	if pb.isprimary || args.FromPrimary {
		pb.log[args.Key] = args.Value
		reply.Err = "ok"
		if !pb.isbackup {
			go func(){
				for {
					args.FromPrimary = true
					ok := call(pb.currentview.Backup, "PBServer.PutAppend", args, &reply)
					if ok == false {
						//fmt.Println("looping")					
						currentview, error := pb.vs.Ping(pb.viewnumber)
						if error == nil {
							pb.currentview = currentview
							pb.viewnumber = currentview.Viewnum
						} 
					} else {
						break
					}
				}
			}()
			
		}
		return nil
	} else {
		var Err error 
		return Err
	}

	

	//fmt.Println(pb.log)

	


	
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	

	currentview, error := pb.vs.Ping(pb.viewnumber)
	if error != nil {
		fmt.Println("Bad ping happened")
	} else {


	pb.currentview = currentview
	pb.viewnumber = currentview.Viewnum

	if currentview.Primary == pb.me {
		pb.isprimary = true
		pb.isbackup = false
	} else if currentview.Backup == pb.me {
		pb.isprimary = false
		pb.isbackup = true
	} else {
		pb.isprimary = false
		pb.isbackup = false
	}

}

}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	fmt.Println("starting the pbserver!")
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.

	pb.log = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
