package pbservice


const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	FromPrimary bool
	SendNum int64
	SendTime int
	IsAppend bool

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	FromPrimary bool
	SendNum int64
	SendTime int
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
