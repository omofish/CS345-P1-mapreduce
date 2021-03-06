package pbservice

import "viewservice"
import "net/rpc"
//import "fmt"

import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	//pb *PBServer
	me string
	server string
	currentview viewservice.View
	viewnum uint
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	//pb := new(PBServer)
	//ck.pb = pb

	return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		//fmt.Println(errx)
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	//fmt.Println("This is the Error:")
	//fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here

	

	args := &GetArgs{}
	args.Key = key
	args.SendNum = nrand()
	args.FromPrimary = false
	
	var reply GetReply

	//fmt.Println("in client get")

	for {
		ok := call(ck.currentview.Primary, "PBServer.Get", args, &reply)
		//fmt.Println(reply.Value)
		//fmt.Println(ok)
		if ok == false {
			//fmt.Println("ok = false")
			currentview, err := ck.vs.Ping(ck.viewnum)
			//fmt.Println("after ping")
			if err == nil {
				//fmt.Println("changing the view in clerk")
				//fmt.Println(ck.me)
				//fmt.Println(currentview)
				ck.currentview = currentview
				ck.viewnum = currentview.Viewnum
			} 
			time.Sleep(viewservice.PingInterval)

			
		} else {
			break
		}
	}

	return reply.Value

	

}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
		
		

		args := &PutAppendArgs{}
		args.Key = key
		
		if op == "Put"{
			args.IsAppend = false
		} else {
			args.IsAppend = true
		}
		args.Value = value
		args.FromPrimary = false
		args.SendNum = nrand()
		
		var reply PutAppendReply
	
		for {
			ok := call(ck.currentview.Primary, "PBServer.PutAppend", args, &reply)
			
			if ok == false {
				currentview, error := ck.vs.Ping(ck.viewnum)
				if error == nil {
					ck.currentview = currentview
					ck.viewnum = currentview.Viewnum
				} 
				time.Sleep(viewservice.PingInterval)
			} else {
				break
			}
		}
	
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
