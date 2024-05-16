package multicomm

import paxoscommm "vp-go-epaxos/paxosCommm"

type MasterInfo struct {
	MasterId       int   //怎么保证这里的值初始化一定不能是0，要不然所有节点的序号不能从0开始
	GenerationId   int   //
	LeaseTimeStamp int64 //有效期，主要不断的续期
}

type MultiNode struct {
	id       int                   //节点id
	priority int                   //节点优先级， 支持通过配置优先级来使得高优先级机器获得master职位
	recv     chan paxoscommm.PaMsg //多通道的channel
	accepted []paxoscommm.PaMsg    // seq -> 已经成立的消息
	g        *paxoscommm.PaGroup
	ms       MasterInfo
}

const (
	FLOW_NOP = iota //定义一些操作，1 删除
	MSG_TYPE_PROPOSE_ACK
	MSG_TYPE_ACCEPT
	MSG_TYPE_ACCEPT_ACK
)

func (m *MultiNode) GetId() int {
	return m.id
}

func (m *MultiNode) IsMaster() bool {
	return m.ms.MasterId == m.id
}

//提交了我肯定就会去处理一下
func (m *MultiNode) BeginCommit(st interface{}) { //返回一个seq的值，首先我不知道当前的最大seq，只能用本地的来开始，如果本地的已经决定了，我才能用下一个
	//先判断当前我是不是主
	var msg MultiMsg
	msg.Body = st
	if !m.IsMaster() {
		//递交给master节点去决定
		m.g.Sendto(m.ms.MasterId, msg)
	}

	//直接放在本地来处理

}

func (m *MultiNode) Recv(t interface{}) {

}

func (m *MultiNode) Propose(t MultiMsg) {

}

//普通正常的accept
func (m *MultiNode) Accept(t MultiMsg) {

}
