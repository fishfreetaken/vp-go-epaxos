package paxoscommm

import "fmt"

type VoteInfo struct {
	ProposeId   int
	Seq         int
	ProposeVote int //提议阶段的选择
	AcceptVote  int //接受阶段的选择
	CommitVote  int //最终提交的值
	FromId      int
}

const (
	PAXOS_MSG_NONE         = iota //没有状态
	PAXOS_MSG_PROPOSE_ING         //正在发起propose的流程中
	PAXOS_MSG_ACCEPT_ING          //开始发起accept流程
	PAXOS_MSG_HAS_ACCEPTED        //已经接受了，这里是确定提议
	PAXOS_MSG_HAS_COMMITED        //已经提交，有一些节点可能被其他的accept，所以需要commit阶段进行提交
)

type MsgRes struct {
	icode int
	msg   string
}

type PaMsg interface {
	Propose(t PaMsg) //默认设置好ack的状态
	Accept(t PaMsg)
	AcceptAck(t PaMsg)
	ProposeAck(t PaMsg) bool

	GetVoteInfo() VoteInfo

	IsAccept() bool

	GetSeqID() int
	SwapFromAndTo() //交换from和to
}

type ClientReq struct {
	Seq  int //事务版本  如果有了需要查询这个事务是否完成
	Body interface{}
}

type ClientRsp struct {
	Seq   int //事务版本号
	State int //0 success ，1 失败，-1 超时，状态未知
}

type PaCommnMsg struct {
	Vt   VoteInfo
	From int

	State int32

	Flowtype int32

	InForm chan int //通知client端的chan消息

	ProposeList []VoteInfo
	AcceptList  []VoteInfo

	Body interface{} //可以是任何种类的数据，
}

//接受一个提议
func (m *PaCommnMsg) Propose(t *PaCommnMsg) {
	if m.IsAccept() || t.Vt.ProposeId <= m.Vt.ProposeId {
		//已经接受了，不需要接收新的提议
		t.Vt = m.Vt
		return
	}
	//更新本地所要投票的对手
	m.Vt = t.Vt
}

func (m *PaCommnMsg) Reset() {
	//重置一个新的结构体
	m = &PaCommnMsg{}
}

//所有的节点编号从1开始
func (m *PaCommnMsg) IsAccept() bool {
	return m.State >= PAXOS_MSG_HAS_ACCEPTED
}

func (m *PaCommnMsg) GetSeqID() int {
	return m.Vt.Seq
}

func (m *PaCommnMsg) SetFrom(id int, flowtype int32) {
	m.Vt.FromId = id
	m.Flowtype = flowtype
}

func (m *PaCommnMsg) ProposeAck(t *PaCommnMsg, membersNum int) (accept bool) {
	if m.IsAccept() {
		//已经接受了的事件，就不需要再处理了
		return
	}
	if t.Vt.ProposeId < m.Vt.ProposeId {
		//特别异常的一个情况，只有冲突的时候才会有这种情况
		panic(fmt.Sprintf("remote:%d_local:%d", t.Vt.ProposeId, m.Vt.ProposeId))
	} else if t.Vt.ProposeId > m.Vt.ProposeId {
		//对方的ProposeId比自己的大 ，todo 放弃或者择机重新发起
		// 三个策略：
		// 1 冲突放弃；2 冲突重试；3 冲突接受(算是我学习到了这个值)
		m.ProposeList = m.ProposeList[:0]
		return
	}

	if t.Vt.ProposeVote != m.Vt.ProposeVote {
		//有人的提议跟我一样，但是proposevote不一样，这个必须要走重试的流程，不然就emo了
		return
	}

	//更新本地的list
	for _, v := range m.ProposeList {
		if v.FromId == t.Vt.FromId {
			//nop 什么也不做
			return
		}
	}

	m.ProposeList = append(m.ProposeList, t.Vt)
	//是否可以进入accept状，我自己有可能投票给别人
	if len(m.ProposeList) >= (membersNum>>1 + 1) {
		//这里采用的是自己先accept再，让其他人继续accept ，默认是自己已经accept
		accept = true
		return
	}
	return
}

func (m *PaCommnMsg) Accept(t *PaCommnMsg) {
	if m.IsAccept() || t.Vt.ProposeId <= m.Vt.ProposeId {
		t.Vt = m.Vt
		return
	}
	m.State = PAXOS_MSG_HAS_ACCEPTED
	m.Vt = t.Vt
}

func (m *PaCommnMsg) AcceptAck(t *PaCommnMsg, membernum int) (accept bool) {
	if !m.IsAccept() {
		//本身要先accept才能ack其他值
		panic("invalid")
	}

	//所有的值都要处理
	for _, v := range m.AcceptList {
		if v.FromId == t.Vt.FromId {
			//重复的
			return
		}
	}

	m.AcceptList = append(m.AcceptList, t.Vt)
	var mpvote = make(map[int]int) //vote
	for _, v := range m.AcceptList {
		mpvote[v.AcceptVote]++
	}
	for vote, votenum := range mpvote {
		if votenum >= (membernum>>1 + 1) {
			//可以提交了接受一个值成功了
			accept = true
			m.Vt.CommitVote = vote
			m.State = PAXOS_MSG_HAS_COMMITED
			break
		}
	}

}
