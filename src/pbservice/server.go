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
	seenMap    map[int64]bool

	currentview viewservice.View

}



func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.

	//fmt.Println("in get!");

	//fmt.Println(pb.log)
	//fmt.Println(args.Key)

	
	


	if pb.isprimary {
		pb.mu.Lock()
		reply.Err = "ok"
		reply.Value = pb.log[args.Key]
		pb.mu.Unlock()
		return nil
	} else {
		//var Err error
		fmt.Println("Fdfds")
		return fmt.Errorf("bad get")
	}
	


	
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.

	//fmt.Println("in PutAppend!");

	//fmt.Println(pb.log)
	fmt.Println("in putappend")
	if args.Value == "33" {
		fmt.Println("Here is 33")
	}
	//fmt.Println(pb.currentview.Backup == pb.me)
	//fmt.Println(pb.seenMap[args.SendNum])

	if !pb.isbackup && !pb.isprimary {
		//var Err error
		fmt.Println("I hope this is the problem")
		return fmt.Errorf("Backup not online")
	}

	if pb.seenMap[args.SendNum] == true {
		fmt.Println("rejecting a putappend")
		return nil
	}

	

	if pb.isprimary {
		
		args.FromPrimary = true
		//fmt.Println("primary is sending this sendnum")
		//fmt.Println(args.SendNum)
		if args.Value == "33" {
			fmt.Println("Here is 33 in isprimary")
		}
		pb.mu.Lock()
		if pb.currentview.Backup != "" {
			if args.Value == "33" {
				fmt.Println("Here is 33 in isprimary in backup send")
			}
			for {
				ok := call(pb.currentview.Backup, "PBServer.PutAppend", args, &reply)
				if ok == false {
					fmt.Println("bad send to backup")
				} else {
					fmt.Println("This was okayed")
					fmt.Println(args.Key)
					fmt.Println(args.Value)
					break
				}
			}
		}
		pb.mu.Unlock()
		
		pb.mu.Lock()
		pb.log[args.Key] = args.Value
		pb.seenMap[args.SendNum] = true
		pb.mu.Unlock()
		return nil
	} else if args.FromPrimary && pb.isbackup{
		pb.mu.Lock()
		pb.log[args.Key] = args.Value
		pb.seenMap[args.SendNum] = true
		pb.mu.Unlock()
		//fmt.Println("putappend for backup")
		//fmt.Println(pb.currentview.Backup == pb.me)
		//fmt.Println(pb.currentview.Primary == pb.me)
		//fmt.Println(args.SendNum)
		//fmt.Println(args.Key)
		return nil
	}
	
	return fmt.Errorf("something went wrong in putappend")

	/*

	if pb.isprimary || args.FromPrimary {
		pb.log[args.Key] = args.Value
		reply.Err = "ok"
		if !pb.isbackup && pb.currentview.Backup != "" {
			//go func(){
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
			//}()
			
		}
		return nil
	} else {
		var Err error 
		return Err
	}

	*/

	

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
		
		if pb.currentview.Backup != currentview.Backup && pb.isprimary {
			//var key string
			//var val interface {}
			//pb.currentview.Backup = currentview.Backup
			pb.mu.Lock()
			fmt.Println(pb.log)
			fmt.Println(pb.me)
			fmt.Println(pb.currentview.Backup)
			fmt.Println(pb.currentview.Primary)
			fmt.Println(currentview.Primary)
			fmt.Println(currentview.Backup)
			var i int64
			i = 0
			for key, val := range pb.log {
				args := &PutAppendArgs{}
				var reply PutAppendReply
				args.FromPrimary = true
				args.Key = key
				args.Value = val
				fmt.Println(key)
				fmt.Println(val)
				fmt.Println(args.SendNum)
				fmt.Println(i)
				args.SendNum = i
				fmt.Println(args.SendNum)
				i++
				fmt.Println("sending vals to backup")
				for {
					ok := call(currentview.Backup, "PBServer.PutAppend", args, &reply)
					if ok == false {
						fmt.Println("could not sent val to backup")
					} else {
						break
					}
				}
				
			}
			pb.mu.Unlock()
		}

		if pb.currentview != currentview {

			pb.mu.Lock()
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
			pb.mu.Unlock()
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
	pb.seenMap = make(map[int64]bool)

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
