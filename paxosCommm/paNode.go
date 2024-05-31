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

var glocInc int32

type PaNode struct {
	id int //节点id

	priority int

	dict sync.Map //[int64]*PaCommnMsg

	veclk []sync.Mutex

	curseq int64
	g      *PaGroup
}

func (m *PaNode) SetVecLkNums() {
	for i := 0; i < 10; i++ {
		m.veclk = append(m.veclk, sync.Mutex{})
	}
}

func (m *PaNode) NewProPoseMsg(proposeid int) *PaCommnMsg {
	//获取当前进度的seqid
	var iCurSeq int64
	for {
		iCurSeq = atomic.AddInt64(&m.curseq, 1)
		if v, ok := m.dict.Load(iCurSeq); ok {
			//panic(fmt.Sprintf("cur id:%d icurseq:%d inavlid msg:%+v", m.id, iCurSeq, v))
			fmt.Printf("cur id:%d icurseq:%d inavlid msg:%+v", m.id, iCurSeq, v)
		} else {
			fmt.Printf("node:%d find curseq:%d no use", m.id, iCurSeq)
			break
		}
	}

	var cm = PaCommnMsg{
		Vt: VoteInfo{
			ProposeId:   proposeid,
			ProposeVote: m.id,
			Seq:         iCurSeq,
			FromId:      m.id,
		},

		InForm: make(chan int),
	}
	m.dict.Store(iCurSeq, &cm)
	//m.dict[iCurSeq] = &cm
	return &cm
}

const (
	PAXOS_MSG_PROPOSE = iota
	PAXOS_MSG_PROPOSE_ACK
	PAXOS_MSG_ACCEPT
	PAXOS_MSG_ACCEPT_ACK
	PAXOS_MSG_COMMIT //不需要ack
)

func (m *PaNode) NewCommitMsg(t *PaCommnMsg) PaCommnMsg {
	t.Vt.AcceptVote = m.id
	t.Vt.FromId = m.id
	t.Flowtype = PAXOS_MSG_COMMIT
	t.Vt.State = PAXOS_MSG_HAS_COMMITED
	return *t
}

func (m *PaNode) BuildProposeId() int {
	//随机+priorityid
	offset := 10000
	atomic.AddInt32(&glocInc, 1)
	return m.priority*offset + int(glocInc) //rand.Intn(offset)
}

//允许自定义控制proposeid
func (m *PaNode) BeginCommit(req *ClientReq) int64 {
	//有自己构造proposeid的能力
	proposeid := m.BuildProposeId()

	//根据seqid 构建消息
	msg := m.NewProPoseMsg(proposeid)

	m.g.Broadcast(msg)

	//给一个事件的注册中心来注册，等待响应结果，要么等待超时
	r := time.NewTicker(time.Second)
	//设置超时时间
	select {
	case <-r.C:
		fmt.Printf("time out proposeid:%d msg:%+v\n", proposeid, msg)
	case v := <-msg.InForm:
		fmt.Printf("suc finish work proposeid node:%d :%d msgproposeid:%d seq:%d rsp::%d vote:%d\n", m.id, proposeid, msg.Vt.ProposeId, msg.Vt.Seq, v, msg.Vt.AcceptVote)
	}
	defer close(msg.InForm)

	return msg.Vt.Seq
}

func (m *PaNode) Step(t *PaCommnMsg) {
	//fmt.Printf("step except i:%d from:%d seq:%d flowtye:%d\n  ", m.id, t.Vt.FromId, t.GetSeqID(), t.Flowtype)
	//每次要更新最大的seq值
	for {
		tmpseq := atomic.LoadInt64(&m.curseq)
		if tmpseq < int64(t.GetSeqID()) {
			//更新最大的seq
			if atomic.CompareAndSwapInt64(&m.curseq, tmpseq, int64(t.GetSeqID())) {
				break
			}
		} else {
			break
		}
	}

	r := m.GetSeqMsg(t.GetSeqID())

	m.veclk[int(t.GetSeqID())%len(m.veclk)].Lock()
	defer m.veclk[int(t.GetSeqID())%len(m.veclk)].Unlock()

	//减少锁的力度
	switch t.Flowtype {
	case PAXOS_MSG_PROPOSE:
		m.Propose(t, r)
	case PAXOS_MSG_PROPOSE_ACK:
		m.ProposeAck(t, r)
	case PAXOS_MSG_ACCEPT:
		m.Accept(t, r)
	case PAXOS_MSG_ACCEPT_ACK:
		m.AcceptAck(t, r)
	case PAXOS_MSG_COMMIT:
		//todo 收到这个消息才算确认最终的值
	}
}

//传指针
func (m *PaNode) Propose(t *PaCommnMsg, r *PaCommnMsg) {
	//提议阶段InForm
	FromId := t.Vt.FromId

	if t.IsAccept() {
		panic(fmt.Sprintf("invalid type m:%+v t:%+v fromid:%d %+v", r, t, t.Vt.FromId, m.g.list[t.Vt.FromId].GetSeqMsg(t.Vt.Seq)))
	}

	r.Propose(t)

	t.SetFrom(m.id, PAXOS_MSG_PROPOSE_ACK)
	//这个肯定是要ack的
	go m.g.Sendto(FromId, *t)
}

func (m *PaNode) Accept(t *PaCommnMsg, r *PaCommnMsg) {
	if !t.IsAccept() {
		//自己都不accept，还要找我
		panic(fmt.Sprintf("id:%d tmsg:%+v", m.id, t))
	}
	FromId := t.Vt.FromId
	r.Accept(t)

	t.SetFrom(m.id, PAXOS_MSG_ACCEPT_ACK)

	go m.g.Sendto(FromId, *t)

}

func (m *PaNode) ProposeAck(t *PaCommnMsg, r *PaCommnMsg) {
	if r.IsAccept() {
		//自己状态已经改变，拒绝
		return
	}

	if t.IsAccept() {
		//异常已经accept的情况
		fmt.Printf("iProposeAck has accepvote loc:%d seq:%d r:%+v t:%+v\n", m.id, t.Vt.Seq, r.Vt, t.Vt)
		tProposeId := t.Vt.ProposeId
		//更新这个t的accept值
		//这时候是强制要接受这个值的，不需要比较proposeid
		t.Vt.UpdateProposeid(&r.Vt)

		if t.Vt.AcceptVote == m.id {
			//我自己都没有accept，远处传来一个accept，肯定失败的
			panic(fmt.Sprintf("seq:%d self:%d", t.Vt.Seq, m.id))
		}

		if !r.Accept(t) {
			panic("accept failed")
		}
		if r.Vt.ProposeId > tProposeId {
			if !t.IsAccept() || !r.IsAccept() {
				panic(fmt.Sprintf("loc:%d t:%+v r:%+v", m.id, t, r))
			}
			//只能帮忙广播一次，最大的值进行广播，不一定会有广播，只是原封不动将这个t转发出去
			//不一定是提出者的
			if m.id != t.Vt.AcceptVote {
				t.SetFrom(t.Vt.AcceptVote, PAXOS_MSG_ACCEPT)
				fmt.Printf("Async go help accept loc:%d seq:%d t:%+v\n", m.id, t.Vt.Seq, t)
				go m.g.Broadcastexcept(*t, m.id)
			}
		}
		//这里还不能决定到底有没有成功，只是当此的提交已经失败了
		r.Inform(0 - t.Vt.AcceptVote)
		return
	}

	//正常的提交流程
	bAccept := r.ProposeAck(t, m.g.GetNumber(), m.id)
	//如果进入到下一个阶段，需要发起broadcast
	if bAccept {
		//这里只能是接收自己
		//接上accept的逻辑，挡住后边的请求，自己先accept
		go m.g.Broadcastexcept(*r, m.id)
	}
}

func (m *PaNode) AcceptAck(t *PaCommnMsg, r *PaCommnMsg) {
	//将数据写入本地的msg里
	bAccept, acceptVoted := r.AcceptAck(t, m.g.GetNumber()) // 返回是否落盘
	//判断是否能进入到下一个阶段
	if bAccept {
		//这里有并发的问题，并不知道是否已经close，超时也是有可能会在if判断之后，close的
		if acceptVoted == m.id {
			r.Inform(int(acceptVoted))
		}
		//todo commit 提交所有人的决议
		fmt.Printf("commit done id:%d seq:%d acceptVoted:%d vt:%+v  r:%+v \n", m.id, t.Vt.Seq, acceptVoted, t.Vt, r.Vt)
		//m.g.InformVoteResult(t.Vt.Seq, acceptVoted, m.id)
		m.g.InformVoteResult(VoteInfo{
			Seq:         r.Vt.Seq,
			ProposeId:   r.Vt.ProposeId,
			ProposeVote: r.Vt.ProposeVote,
			AcceptVote:  acceptVoted,
		})
	}
}

func (m *PaNode) GetSeqMsg(seq int64) *PaCommnMsg {
	v, exist := m.dict.Load(seq)
	if !exist {
		//创建一个新的msg
		var msgbody = PaCommnMsg{
			Vt: VoteInfo{
				Seq:    seq,
				FromId: m.id,
			},
		}
		actMsgBody, _ := m.dict.LoadOrStore(seq, &msgbody)
		/*
			if ok {
				//fmt.Printf("GetSeqMsg Fialed seq:%d actmsgbody：%+v\n", seq, actMsgBody)
			}
		*/
		return actMsgBody.(*PaCommnMsg)
	}
	return v.(*PaCommnMsg)
}

func (m *PaNode) GetId() int {
	return m.id
}

func (m *PaNode) Recv(t PaCommnMsg) {
	go m.Step(&t)
}
