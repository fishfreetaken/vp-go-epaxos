package paxoscommm

import "fmt"

const (
	PAXOS_MSG_BEGIN_PROPOSE = iota //正在发起propose的流程中
	PAXOS_MSG_HAS_ACCEPTED         //已经接受了，这里是确定提议
	PAXOS_MSG_HAS_COMMITED         //已经提交，有一些节点可能被其他的accept，所以需要commit阶段进行提交
)

type VoteInfo struct {
	ProposeId   int
	Seq         int64
	ProposeVote int //提议阶段的选择
	AcceptVote  int //接受阶段的选择
	CommitVote  int //最终提交的值
	FromId      int
	State       int32
}

func (m *VoteInfo) UpdateProposeid(t *VoteInfo) {
	if t.ProposeId > m.ProposeId {
		m.ProposeId = t.ProposeId
		m.ProposeVote = t.ProposeVote
	}
}

func (m *VoteInfo) IsAccept() bool {
	return m.State >= PAXOS_MSG_HAS_ACCEPTED
}

func (m *VoteInfo) IsCommit() bool {
	return m.State >= PAXOS_MSG_HAS_COMMITED
}

func (m *VoteInfo) SetFrom(id int) {
	m.FromId = id
}

func (m *VoteInfo) SetPropose(t *VoteInfo) bool {
	if t.IsAccept() {
		panic(fmt.Sprintf("invalid type m:%+v t:%+v", m, t))
	}
	if m.IsAccept() || t.ProposeId < m.ProposeId {
		t.UpdateProposeid(m)
		t.AcceptVote = m.AcceptVote
		t.State = m.State
		return false
	}

	m.UpdateProposeid(t)
	//fmt.Printf("set propose debug %+v t:%+v\n", m, t)
	return true
}

func (m *VoteInfo) ResetState() {
	m.State = PAXOS_MSG_BEGIN_PROPOSE
	m.AcceptVote = -1
}

func (m *VoteInfo) SetAccept(t *VoteInfo) (suc bool) {
	if !t.IsAccept() {
		panic(fmt.Sprintf("no valid t local proposeid:%d t:%+v\n", m.ProposeId, t))
	}

	defer func() {
		t.UpdateProposeid(m)
	}()

	m.UpdateProposeid(t)

	/*
		if t.ProposeId == m.ProposeId && t.ProposeVote != m.ProposeVote {
			panic(fmt.Sprintf("uniq proposeid vote diff t:%+v m:%+v\n", t, m))
		}
	*/

	if m.IsAccept() {
		//有可能已经commit,所以要赋值给state
		t.State = m.State
		t.AcceptVote = m.AcceptVote
		t.CommitVote = m.CommitVote
		return
	}

	//不接受低的proposid的accept
	if m.ProposeId > t.ProposeId {
		//t的状态需要重置
		t.ResetState()
		return
	}
	suc = true
	//真正的accept
	m.AcceptVote = t.AcceptVote
	m.State = PAXOS_MSG_HAS_ACCEPTED

	return
}

func (m *VoteInfo) SetCommit(commitVote int) {
	if m.IsCommit() {
		//不允许修改
		return
	}
	//commite是最终决定的值
	m.CommitVote = commitVote
	m.State = PAXOS_MSG_HAS_COMMITED
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
	Vt VoteInfo

	Flowtype int32 //最高位置进行标记，节省内存

	ProposeList []VoteInfo
	AcceptList  []VoteInfo

	Body interface{} //可以是任何种类的数据，
}

//所有的节点编号从1开始
func (m *PaCommnMsg) IsAccept() bool {
	return m.Vt.IsAccept()
}

func (m *PaCommnMsg) IsCommit() bool {
	return m.Vt.State >= PAXOS_MSG_HAS_COMMITED
}

func (m *PaCommnMsg) GetSeqID() int64 {
	return m.Vt.Seq
}

func (m *PaCommnMsg) SetFrom(id int, flowtype int32) {
	m.Vt.SetFrom(id)
	m.Flowtype = flowtype
}

//接受一个提议
func (m *PaCommnMsg) Propose(t *PaCommnMsg) {

	m.Vt.SetPropose(&t.Vt)
}

func (m *PaCommnMsg) ProposeAck(t *PaCommnMsg, membersNum, nodeid int) (accept bool) {

	if m.Vt.IsAccept() {
		//我已经accept了，没有意义接受这个阶段
		return
	}

	if t.Vt.IsAccept() {
		m.Vt.SetAccept(&t.Vt)
		//通知下游已经失败
		return
	}

	if t.Vt.ProposeId < m.Vt.ProposeId {
		//在做重试之前，应该不能出现这种情况
		//特别异常的一个情况，只有冲突的时候才会有这种情况
		// 我更新了一个最大的proposeid，但是后边又来了一个旧值，所以这种情况还是允许发生的
		//panic(fmt.Sprintf("smaller proporse than local seq:%d remote %d:%d_local:%d:%d\n", t.Vt.Seq, t.Vt.FromId, t.Vt.ProposeId, m.Vt.FromId, m.Vt.ProposeId))
		return
	} else if t.Vt.ProposeId > m.Vt.ProposeId {
		//对方的ProposeId比自己的大 ，todo 放弃或者择机重新发起
		// 三个策略：
		// 1 冲突放弃；2 冲突重试；3 冲突接受(算是我学习到了这个值)
		//fmt.Printf("bigger proporse than local seq:%d  remote %d:%d_local:%d:%d\n", t.Vt.Seq, t.Vt.FromId, t.Vt.ProposeId, m.Vt.FromId, m.Vt.ProposeId)
		m.ProposeList = m.ProposeList[:0]

		//先接受这个提议，挡住后边的accept请求,,如果收到醉倒的proposeid的话，一定要更新proposeid
		//m.Vt.ProposeId = t.Vt.ProposeId
		//fmt.Printf("SetPropose before nodeid:%d :%+v %+v", nodeid, t, m)
		m.Vt.SetPropose(&t.Vt)
		//fmt.Printf(" localnodid:%d update t:%+v m:%+v\n", nodeid, t, m)
		return
	} else if t.Vt.ProposeVote != m.Vt.ProposeVote {
		//这里要保证ProposeId唯一性
		panic(fmt.Sprintf("localnodie:%d t:%+v  m:%+v\n", nodeid, t, m))
	}

	//if m.Vt.ProposeVote != nodeid {
	//选举的不是自己，放弃
	//fmt.Printf("not propose vote myself nodeid:%d :%+v %+v", nodeid, m.Vt, t.Vt)
	//}

	//更新本地的list
	for _, v := range m.ProposeList {
		if v.FromId == t.Vt.FromId {
			//nop 什么也不做
			//将这条清理掉
			/*
				if v.State < t.Vt.State {
					m.ProposeList = append(m.ProposeList[0:idx], m.ProposeList[idx+1:]...)
					break
				} else {
					return
				}
			*/
			return
		}
	}
	//这里还是要感知到自己的失败的
	m.ProposeList = append(m.ProposeList, t.Vt)
	var cnt int
	for _, v := range m.ProposeList {
		if v.ProposeVote == nodeid {
			cnt++
		}
	}
	//是否可以进入accept状，我自己有可能投票给别人
	if cnt >= (membersNum>>1 + 1) {
		//这里采用的是自己先accept再，让其他人继续accept ，默认是自己已经accept
		accept = true

		//只能接受自己
		m.Vt.State = PAXOS_MSG_HAS_ACCEPTED
		m.Vt.AcceptVote = nodeid

		//先接受自己
		m.AddAccepList(m.Vt)
		m.Flowtype = PAXOS_MSG_ACCEPT
	}
	return
}

func (m *PaCommnMsg) AddAccepList(t VoteInfo) {
	for _, v := range m.AcceptList {
		if v.FromId == t.FromId {
			panic(fmt.Sprintf("impossble:%+v", m))
		}
	}
	m.AcceptList = append(m.AcceptList, t)
}

func (m *PaCommnMsg) Accept(t *PaCommnMsg) bool {
	return m.Vt.SetAccept(&t.Vt)
}

func (m *PaCommnMsg) AcceptAck(t *PaCommnMsg, membernum int) (accept bool, acceptvote int) {
	//if !t.Vt.IsAccept() || !m.Vt.IsAccept() {
	if !m.Vt.IsAccept() {
		//对方不会将t的状态进行降级
		//我请求了对方，但是对方认为你的proposeid比较低而且不是accept的状态，可能还是处在propose的状态，但是我已经accept了，不需要你，拒绝了你
		panic(fmt.Sprintf("cur id:%+v invalid accept:%+v", m, t))
	}

	if m.Vt.IsCommit() {
		return
	}

	if t.Vt.IsCommit() {
		m.Vt.SetCommit(t.Vt.CommitVote)
		//清理一下当前的存储
		m.AcceptList = m.AcceptList[:0]
		accept = true
		acceptvote = t.Vt.CommitVote
		return
	}

	//时刻保持最大proposeid
	m.Vt.UpdateProposeid(&t.Vt)

	if !t.Vt.IsAccept() {
		//对方没有接受，因为proposeid小,就不需要进入队列了
		return
	}

	bFind := false
	for _, v := range m.AcceptList {
		if v.FromId == t.Vt.FromId {
			bFind = true
			return
		}
	}

	if !bFind {
		m.AcceptList = append(m.AcceptList, t.Vt)
	}

	//所有的值都要处理
	var mpvote = make(map[int]int) //vote
	for _, v := range m.AcceptList {
		if !v.IsAccept() {
			continue
		}
		mpvote[v.AcceptVote]++
	}

	var cnt int
	var maxvale int

	for vote, votenum := range mpvote {
		if votenum >= (membernum>>1 + 1) {
			//可以提交了接受一个值成功了
			m.Vt.SetCommit(vote)
			acceptvote = vote
			accept = true
			//todo broadcase commit
			return
		}
		cnt += votenum
		if maxvale < votenum {
			maxvale = votenum
		}
	}
	if ((membernum - cnt) + maxvale) < (membernum>>1 + 1) {
		//谁都胜利不出
		fmt.Printf("impossiable accept vote success maxvale:%d membernum:%d cnt:%d  m:%+v t:%+v\n", maxvale, membernum, cnt, m, t)
	}
	//要检查是不是肯定不能成功了，检查所有的票状态
	//感知到如果是空洞的话，这里提交的就失败了
	return
}
