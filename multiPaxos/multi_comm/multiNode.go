package multicomm

type MultiNode struct {
	id       int //节点id
	recv     chan MultiMsg
	accepted []MultiMsg // seq -> 已经成立的消息
	g        *Group
}

const (
	MSG_TYPE_PROPOSE = iota
	MSG_TYPE_PROPOSE_ACK
	MSG_TYPE_ACCEPT
	MSG_TYPE_ACCEPT_ACK
)
