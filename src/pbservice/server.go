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
	mu         sync.RWMutex
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
	seenMap    map[string]int64
	timeLog    map[string]time.Time
	backupOrder int
	recOrder	int
	hasBackup  bool

	currentview viewservice.View

}



func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.

	//fmt.Println("in get!");

	//fmt.Println(pb.log)
	//fmt.Println(args.Key)

	//fmt.Println(pb.currentview.Primary)
	//fmt.Println(pb.me)
	
	if pb.isdead() {
		//reply.Err = fmt.Errorf("dead")
		return nil
	}

	if pb.isprimary {
		pb.mu.RLock()
		reply.Err = "ok"
		reply.Value = pb.log[args.Key]
		pb.mu.RUnlock()
		return nil
	} else {
		//var Err error
		//fmt.Println("Fdfds")
		return fmt.Errorf("bad get")
	}
	


	
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	if pb.isdead() {
		reply.Err = "dead"
		return nil
	}
	
	//fmt.Print(pb.me + " ")
	//fmt.Println(args)

	
	pb.mu.Lock()
	if(pb.seenMap[args.Key] == args.SendNum && !args.NewBackup){
		pb.mu.Unlock()
		return nil
	}
	
	pb.mu.Unlock()
	

	if pb.isprimary {
		pb.mu.Lock()
		if(args.IsAppend){
			args.Value = pb.log[args.Key] + args.Value
		}
		pb.log[args.Key] = args.Value
		pb.seenMap[args.Key] = args.SendNum
		
		args.FromPrimary = true
		if pb.hasBackup {
			//go func(){
				var i int
				for {
					
					ok := call(pb.currentview.Backup, "PBServer.PutAppend" , args, &reply)
					
					
					if ok != false {
						break
					}
					if i > 20 {
						break
					}
					time.Sleep(viewservice.PingInterval)
					i++
					
					
				}
			//}()
			
		}
		pb.mu.Unlock()
		
		return nil
		
	} else if pb.isbackup {
		//fmt.Println(pb.me + "Backup?")
		//fmt.Println(args.NewBackup)
		if args.NewBackup == true {
			//fmt.Println("recieving backup dump")
			pb.mu.Lock()
			for key, val := range args.BackupDump{
				fmt.Println("backupdumpwrite")
				pb.log[key] = val
			}
			pb.mu.Unlock()
		}
		if args.FromPrimary {
			pb.mu.Lock()
			pb.log[args.Key] = args.Value
			pb.seenMap[args.Key] = args.SendNum
			pb.mu.Unlock()
		}
		return nil
	}

	
	return fmt.Errorf("bad putappend")


	
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	//fmt.Println("tring to ticking")
	

	currentview, error := pb.vs.Ping(pb.viewnumber)
	if error != nil {
		return
	} 

	if currentview != pb.currentview {
		//fmt.Println(currentview.Backup)
		
		if currentview.Backup != pb.currentview.Backup && pb.isprimary{
			pb.hasBackup = true
			args := &PutAppendArgs{}
			var reply GetReply
			args.BackupDump = pb.log
			args.NewBackup = true
			//fmt.Println(pb.me + " dumping to backup")
			pb.mu.Lock()
			//go func() {
				for {
					ok := call(currentview.Backup, "PBServer.PutAppend", args, &reply)
					if ok != false {
						break
					}
					time.Sleep(viewservice.PingInterval)
				}
			//}()
			pb.mu.Unlock()
			
		}
		
		
		pb.mu.Lock()
		pb.currentview = currentview
		pb.viewnumber = currentview.Viewnum
		

		if pb.currentview.Primary == pb.me {
			pb.isprimary = true
			pb.isbackup = false
		} else if pb.currentview.Backup == pb.me {
			pb.isprimary = false
			pb.isbackup = true
		} else {
			pb.isbackup = false
			pb.isprimary = false
		}
		pb.mu.Unlock()
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
	//fmt.Println("starting the pbserver!")
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.

	pb.backupOrder = 0
	pb.recOrder = -1
	pb.hasBackup = false
	pb.log = make(map[string]string)
	pb.seenMap = make(map[string]int64)
	pb.timeLog = make(map[string]time.Time)

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
