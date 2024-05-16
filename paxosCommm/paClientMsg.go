package paxoscommm

const (
	OP_NOP         = iota //一些空操作，比如说加一些心跳之类的
	OP_BE_MASTER          //内部的一些操作
	OP_CLIENT_WORK        //客户端发起的事务，
)

type PaClientMsg struct {
	UniqId    string //client段定义，全局唯一 幂等，如果是空的话，不需要保证幂等
	SeqId     string //事件的编号,全局唯一
	Op        int32
	Operation string //自定义操作解析
}
