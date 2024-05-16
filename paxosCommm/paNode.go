package paxoscommm

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MSG_TYPE_PROPOSE = iota
	MSG_TYPE_PROPOSE_ACK
	MSG_TYPE_ACCEPT
	MSG_TYPE_ACCEPT_ACK
)

type PaNode struct {
	id   int             //节点id
	recv chan PaCommnMsg //多通道的channel

	dict map[int]*PaCommnMsg

	veclk []sync.Mutex

	curseq int64
	g      PaGroup
}

type PaNodeInterface interface {
	BeginCommit(proposeid int, body PaClientMsg) //这里是发起一个提议,proposeid,需要绑定一个proposeid，如果是0的话，这里本地可以自动生成一个

	Recv(t PaMsg)
	AsyncReceive() //异步监听自己的事件

	Propose(t PaMsg)
	Accept(t PaMsg)
	ProposeAck(t PaMsg) bool
	AcceptAck(t PaMsg)

	//这里拿到的是指针，需要自动的有落盘的策略
	GetSeqMsg(seq int) PaMsg

	GetId() int //获取节点id
}

func (m *PaNode) NewProPoseMsg(proposeid int) *PaCommnMsg {
	//获取当前进度的seqid
	var iCurSeq int
	for iCurSeq <= int(atomic.LoadInt64(&m.curseq)) {
		icurtmp := atomic.LoadInt64(&m.curseq)
		iCurSeq = iCurSeq + 1
		if atomic.CompareAndSwapInt64(&m.curseq, icurtmp, int64(iCurSeq)) {
			break
		}
	}

	var cm = PaCommnMsg{
		Vt: VoteInfo{
			ProposeId: proposeid,
			Seq:       int(iCurSeq),
		},
		From:   m.id,
		InForm: make(chan int),
	}
	m.dict[iCurSeq] = &cm
	return &cm
}

const (
	PAXOS_MSG_PROPOSE = iota
	PAXOS_MSG_PROPOSE_ACK
	PAXOS_MSG_ACCEPT
	PAXOS_MSG_ACCEPT_ACK
	PAXOS_MSG_COMMIT //不需要ack
)

func (m *PaNode) NewAcceptMsg(t *PaCommnMsg) *PaCommnMsg {
	t.Vt.AcceptVote = m.id
	t.Vt.FromId = m.id
	t.Flowtype = 
	return t
}

//允许自定义控制proposeid
func (m *PaNode) BeginCommit(proposeid int, req *ClientReq) {

	//根据seqid 构建消息
	msg := m.NewProPoseMsg(proposeid)

	m.g.Broadcast(msg)

	//给一个事件的注册中心来注册，等待响应结果，要么等待超时
	r := time.NewTicker(time.Second)
	//设置超时时间
	select {
	case <-r.C:
		fmt.Printf("time out proposeid:%d", proposeid)
	case <-msg.InForm:
		fmt.Printf("suc finish work proposeid :%d", proposeid)
	}
	defer close(msg.InForm)
}

func (m *PaNode) Step(t *PaCommnMsg) {
	m.veclk[t.GetSeqID()%len(m.veclk)].Lock()
	defer m.veclk[t.GetSeqID()%len(m.veclk)].Unlock()

	//减少冲突事件
	for atomic.LoadInt64(&m.curseq) < int64(t.GetSeqID()) {
		tmpseq := m.curseq
		//更新最大的seq
		if atomic.CompareAndSwapInt64(&m.curseq, tmpseq, int64(t.GetSeqID())) {
			break
		}
	}
	//减少锁的力度
	switch t.Flowtype {
	case PAXOS_MSG_PROPOSE:
		m.Propose(t)
	case PAXOS_MSG_PROPOSE_ACK:
		m.ProposeAck(t)
	case PAXOS_MSG_ACCEPT:
		m.Accept(t)
	case PAXOS_MSG_ACCEPT_ACK:
		m.AcceptAck(t)
	case PAXOS_MSG_COMMIT:
		//todo 收到这个消息才算确认最终的值
	}
}

//传指针
func (m *PaNode) Propose(t *PaCommnMsg) {
	//提议阶段
	FromId := t.Vt.FromId
	r := m.GetSeqMsg(t.GetSeqID())
	r.Propose(t)
	t.SetFrom(m.id, PAXOS_MSG_PROPOSE_ACK)
	go m.g.Sendto(FromId, *t)
}

func (m *PaNode) Accept(t *PaCommnMsg) {
	FromId := t.Vt.FromId
	r := m.GetSeqMsg(t.GetSeqID())
	r.Accept(t)

	t.SetFrom(m.id, PAXOS_MSG_ACCEPT_ACK)
	go m.g.Sendto(FromId, *t)
}

func (m *PaNode) ProposeAck(t *PaCommnMsg) {
	r := m.GetSeqMsg(t.GetSeqID())
	//将数据写入本地的msg里
	bAccept := r.ProposeAck(t, m.g.GetNumber()) // 返回是否落盘
	//如果进入到下一个阶段，需要发起broadcast

	if bAccept {
		//只能广播一次
		go m.g.Broadcast(m.NewAcceptMsg(t))
	}
}

func (m *PaNode) AcceptAck(t *PaCommnMsg) {
	r := m.GetSeqMsg(t.GetSeqID())
	//将数据写入本地的msg里
	bAccept := r.AcceptAck(t, m.g.GetNumber()) // 返回是否落盘
	//判断是否能进入到下一个阶段
	if bAccept {
		//todo commit 提交所有人的决议
	}
}

func (m *PaNode) GetSeqMsg(seq int) *PaCommnMsg {
	v, exist := m.dict[seq]
	if !exist {
		//创建一个新的msg
		var msgbody = PaCommnMsg{
			Vt: VoteInfo{
				Seq: seq,
			},
		}
		m.dict[seq] = &msgbody
	}
	return v
}

func (m *PaNode) GetId() int {
	return m.id
}

func (m *PaNode) AsyncReceive() {
	for v := range m.recv {
		//异步执行，涉及到并行的问题，提高并行度
		go m.Step(&v)
	}
}
