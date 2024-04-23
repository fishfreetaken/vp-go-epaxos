package paxoscommm

type PaNode interface {
	ToBeMaster(seq int) //开启广播的一个事件，这里要设置一个超时

	AddToMyRecv(t PaMsg)
	AsyncReceive() //异步监听自己的事件

	Getmsg(seq int) PaMsg
	GetMemberNum() int //获取成员的数量
	GetId() int        //获取节点id
}
