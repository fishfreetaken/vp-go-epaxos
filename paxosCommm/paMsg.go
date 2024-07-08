package paxoscommm

import (
	"fmt"
	"math"
)

const (
	PAXOS_MSG_BEGIN_PROPOSE   = iota //正在发起propose的流程中
	PAXOS_MSG_SMALL_PROPOSEID        //拒绝的状态
	PAXOS_MSG_HAS_ACCEPTED           //已经接受了，这里是确定提议
	PAXOS_MSG_HAS_COMMITED           //已经提交，有一些节点可能被其他的accept，所以需要commit阶段进行提交
	PAXOS_MSG_HAS_FAILED             //这次决议已经失败，没有谁获得了成功过
)

type PaxosState struct {
	State     int32
	ProposeId uint32 //这个值是要比较和更新最大的
	Vote      uint32 //选的谁
}

//如果对方比自己状态进一步，我需要用你的状态更新我自己的状态
func (m *PaxosState) RefreshState(t *PaxosState) bool {
	if t.IsPropose() && m.IsPropose() {
		if !m.Check(t) {
			panic(fmt.Sprintf("invalid refresh state m:%+v t:%+v", m, t))
		}
		if m.ProposeId < t.ProposeId {
			m.Vote = t.Vote
			m.ProposeId = t.ProposeId
			return true
		}
		return false
	}

	if m.HasCommit() {
		//本身我已经提交了，就不能继续走下一步用你的状态更新我了
		return false
	}

	//我已经接受过一个值了，最终我只需要更新commit的值就行了
	if m.HasAccept() && !t.HasCommit() {
		return false
	}

	m.ProposeId = t.ProposeId
	m.State = t.State
	m.Vote = t.Vote
	return true
}

func (m *PaxosState) GetVote() uint32 {
	return m.Vote
}
func (m *PaxosState) GetProposeId() uint32 {
	return m.ProposeId
}

func (m *PaxosState) IsPropose() bool {
	return m.State == PAXOS_MSG_BEGIN_PROPOSE
}

func (m *PaxosState) HasAccept() bool {
	return m.State >= PAXOS_MSG_HAS_ACCEPTED
}

func (m *PaxosState) IsAccept() bool {
	return m.State == PAXOS_MSG_HAS_ACCEPTED
}

func (m *PaxosState) HasCommit() bool {
	return m.State >= PAXOS_MSG_HAS_COMMITED
}

func (m *PaxosState) IsCommit() bool {
	return m.State == PAXOS_MSG_HAS_COMMITED
}

func (m *PaxosState) IsFailed() bool {
	return m.State == PAXOS_MSG_HAS_FAILED
}

//accept失败，收到其中小的proposeid的状态
func (m *PaxosState) StepToSmallProposeID(up uint32) {
	m.State = PAXOS_MSG_SMALL_PROPOSEID
	m.ProposeId = up
	m.Vote = math.MaxUint32
}

func (m *PaxosState) StepAccept(vote uint32) bool {
	if m.HasAccept() {
		return false
	}
	m.State = PAXOS_MSG_HAS_ACCEPTED
	m.Vote = vote
	return true
}

func (m *PaxosState) StepCommit(vote uint32) bool {
	if m.HasCommit() {
		return false
	}
	m.State = PAXOS_MSG_HAS_COMMITED
	m.Vote = vote
	return true
}

func (m *PaxosState) StepFailed() bool {
	if m.IsCommit() {
		panic(fmt.Sprintf("step failed has commit:%+v", m))
	}
	if m.IsFailed() {
		return false
	}
	m.State = PAXOS_MSG_HAS_FAILED
	m.Vote = math.MaxUint32
	return true
}

//定义proposeid落后的语意
func (m *PaxosState) IsBehind(p *PaxosState) bool {
	return p.ProposeId > m.ProposeId
}

func (m *PaxosState) Check(t *PaxosState) bool {
	if t.ProposeId == m.ProposeId && t.Vote != m.Vote {
		return false
	}
	//commit只能有一个提交
	if m.IsCommit() && t.IsCommit() && m.Vote != t.Vote {
		return false
	}
	return true
}

//存储选举的最小单位，节省内存开销
type StoreVoteInfo struct {
	Vote   uint32 //选谁
	FromId uint32 //谁选的
}

func (m *StoreVoteInfo) IsSameFrom(fromid uint32) bool {
	return m.FromId == fromid
}

func (m *StoreVoteInfo) IsSameVote(p uint32) bool {
	return m.Vote == p
}

func (m *StoreVoteInfo) GetFromID() uint32 {
	return m.FromId
}
func (m *StoreVoteInfo) GetVote() uint32 {
	return m.Vote
}

func (m *StoreVoteInfo) SetFromID(t uint32) {
	m.FromId = t
}

const (
	PAXOS_MSG_PROPOSE = iota
	PAXOS_MSG_PROPOSE_ACK
	PAXOS_MSG_ACCEPT
	PAXOS_MSG_ACCEPT_ACK
	PAXOS_MSG_COMMIT //不需要ack
)

//交换消息
type SwapMsgVoteInfo struct {
	Flowtype int32 //最高位置进行标记，节省内存
	Seq      uint64
	State    PaxosState //每个位置的状态也不一致
	Fromid   uint32
	Cmd      string //有可能带上执行的命令
}

func (m *SwapMsgVoteInfo) BuildStorageCv() StoreVoteInfo {
	return StoreVoteInfo{
		Vote:   m.State.Vote,
		FromId: m.Fromid,
	}
}

func (m *SwapMsgVoteInfo) GetSeqID() uint64 {
	return m.Seq
}

func (m *SwapMsgVoteInfo) GetFromID() uint32 {
	return m.Fromid
}

func (m *SwapMsgVoteInfo) StepAck(f uint32) uint32 {
	if m.Flowtype == PAXOS_MSG_PROPOSE {
		m.Flowtype = PAXOS_MSG_PROPOSE_ACK
	} else if m.Flowtype == PAXOS_MSG_ACCEPT {
		m.Flowtype = PAXOS_MSG_ACCEPT_ACK
	} else {
		panic(fmt.Sprintf("invalid flow type:%+v", m))
	}
	rawfrom := m.Fromid
	m.Fromid = f
	return rawfrom
}

type ClientReq struct {
	Instanceid uint64 //事务版本  如果有了需要查询这个事务是否完成
	Step       int32  //协助定位问题
	RetryTimes uint32 //只能进行有限次数的重试吧，不能一直重试
	Body       string //任何指令要搞成字符串的形式，自己解析
}

func isImpossible(membernum, curCount, maxNum, passNum uint32) bool {
	if (membernum - curCount + maxNum) < passNum {
		return true
	}
	return false
}

type ProposeInfo struct {
	sucCnt  uint32 //过滤重复的请求，一般来说不应该出现重复的请求，我这里一个请求只会出现一个ack，
	failCnt uint32
}

func (m *ProposeInfo) IncSuc() {
	m.sucCnt++
}

func (m *ProposeInfo) IncFail() {
	m.failCnt++
}

func (m *ProposeInfo) Judge(id, membernum uint32) (r PaxosState) {
	passNum := membernum>>1 + 1
	if m.sucCnt >= passNum {
		r.StepAccept(id)
		return
	}

	if isImpossible(membernum, m.failCnt+m.sucCnt, MaxValue(m.sucCnt, m.failCnt), passNum) {
		fmt.Printf("[Warning]ProposeInfo judge id:%d member:%d faile:%d suc:%d has impossbile", id, membernum, m.failCnt, m.sucCnt)
		r.StepFailed()
		return
	}
	return
}

type AcceptInfo struct {
	votelist []uint32 //这里存各个节点的选择的列表，这里也是不需要过滤重复的
	failCnt  uint32   //那些由于proposeid小于的返回的fail
}

func (m *AcceptInfo) addOne(vote uint32) {
	if IsInValidVote(vote) {
		m.failCnt++
		return
	}
	m.votelist = append(m.votelist, vote)

}

func (m *AcceptInfo) AddAndJudge(vote uint32, membernum uint32) (st PaxosState) {
	m.addOne(vote)

	passNum := membernum>>1 + 1

	cnt := uint32(len(m.votelist)) + m.failCnt

	if cnt > membernum {
		panic(fmt.Sprintf("vote:%d invalid member num:%d", vote, cnt))
	}

	if cnt < passNum {
		//减少计算量
		return
	} else if m.failCnt >= passNum {
		//失败的过半数了
		st.StepFailed()
		return
	}
	var mpvote = make(map[uint32]uint32) //vote
	maxNum := m.failCnt
	//这里看选票是否被瓜分了
	for _, v := range m.votelist {
		mpvote[v]++
		tmpvotenum := mpvote[v]
		if tmpvotenum >= passNum {
			st.StepAccept(v)
			return
		}
		if tmpvotenum > maxNum {
			maxNum = tmpvotenum
		}
	}

	if isImpossible(membernum, cnt, maxNum, passNum) {
		//谁都胜利不出
		st.StepFailed()
		fmt.Printf("impossiable accept vote success maxNum:%d membernum:%d cnt:%d \n", maxNum, membernum, cnt)
	}
	return

}

type PaCommnMsg struct {
	Seq    uint64
	FromId uint32
	State  PaxosState //PAXOS_MSG_BEGIN_PROPOSE

	ProposeRes ProposeInfo
	AcceptRes  AcceptInfo

	Body ClientReq //本地提交的时候会保存这个msg
}

func (m *PaCommnMsg) GetSeqID() uint64 {
	return m.Seq
}

func (m *PaCommnMsg) GetFromID() uint32 {
	return m.FromId
}

func (m *PaCommnMsg) ClearStackMsg() {
	m.AcceptRes.votelist = m.AcceptRes.votelist[:0]
}

//接受一个提议
func (m *PaCommnMsg) Propose(t *SwapMsgVoteInfo) {
	if t.State.HasAccept() || !m.State.Check(&t.State) {
		panic(fmt.Sprintf("invalid type m:%+v t:%+v", m, t))
	}

	if m.State.IsPropose() && m.State.IsBehind(&t.State) {
		m.State = t.State
	}
	//已经accept，没有这个简洁完美的版本，ack还是需要知道m的当前状态的
	t.State = m.State
}

func (m *PaCommnMsg) ProposeAck(t *SwapMsgVoteInfo, membersNum uint32) (resMsg *SwapMsgVoteInfo, retry bool) {

	//避免自己收到自己消息的情况
	if m.FromId == t.GetFromID() {
		panic(fmt.Sprintf("cannot vote myself fromid:%d t:%+v m:%+v", m.GetFromID(), t, m))
	}

	if t.State.IsPropose() {
		if t.State.IsBehind(&m.State) {
			//老的无效数据不考虑了
			//step failed 也能节省请求量
			//在做重试之前，应该不能出现这种情况
			//特别异常的一个情况，只有冲突的时候才会有这种情况
			// 我更新了一个最大的proposeid，但是后边又来了一个旧值，所以这种情况还是允许发生的
			//panic(fmt.Sprintf("smaller proporse than local seq:%d remote %d:%d_local:%d:%d\n", t.Vt.Seq, t.Vt.FromId, t.Vt.ProposeId, m.Vt.FromId, m.Vt.ProposeId))
			return
		} else if m.State.IsBehind(&t.State) {
			//对方的ProposeId比自己的大 ，todo 放弃或者择机重新发起
			// 三个策略：
			// 1 冲突放弃；2 冲突重试；3 冲突接受(算是我学习到了这个值)
			//fmt.Printf("bigger proporse than local seq:%d  remote %d:%d_local:%d:%d\n", t.Vt.Seq, t.Vt.FromId, t.Vt.ProposeId, m.Vt.FromId, m.Vt.ProposeId)
			//先接受这个提议，挡住后边的accept请求,,如果收到醉倒的proposeid的话，一定要更新proposeid
			//m.Vt.ProposeId = t.Vt.ProposeId
			//fmt.Printf("SetPropose before nodeid:%d :%+v %+v", nodeid, t, m)
			//遇到小的值退出
			m.State.StepToSmallProposeID(t.State.GetProposeId())
			retry = true
			//这里也是要触发一个新的提交流程
			//fmt.Printf(" localnodid:%d update t:%+v m:%+v\n", nodeid, t, m)
			return
		}
		if !m.State.Check(&t.State) {
			//这里要保证ProposeId唯一性
			// 如果proposeid相同，vote竟然不同，太不可思议了
			panic(fmt.Sprintf("localnodie:%d t:%+v  m:%+v\n", m.GetFromID(), t, m))
		}
		m.ProposeRes.IncSuc()
	} else if t.State.HasAccept() {
		if t.State.HasCommit() || !t.State.IsBehind(&m.State) {
			//接受这个值
			m.State.StepAccept(t.State.GetVote())
			retry = true
			return
		} else {
			//已经accept的，但是proposeid比你落后
			m.ProposeRes.IncFail()
		}
	} else {
		panic(fmt.Sprintf(" proposeack imposs state invalid :%d t:%+v  m:%+v\n", m.GetFromID(), t, m))
	}

	rt := m.ProposeRes.Judge(m.FromId, membersNum)
	if rt.IsFailed() {
		m.State.StepFailed()
	} else if rt.HasAccept() {
		m.State.StepAccept(m.FromId)
		resMsg = m.BuildSwapMsg(PAXOS_MSG_ACCEPT_ACK)
		m.AcceptAck(resMsg, membersNum)
	}
	return
}

func (m *PaCommnMsg) BuildSwapMsg(flowType int32) *SwapMsgVoteInfo {
	return &SwapMsgVoteInfo{
		Flowtype: flowType,
		Seq:      m.Seq,
		State:    m.State,
		Fromid:   m.FromId,
		Cmd: func() string {
			if flowType == PAXOS_MSG_ACCEPT || flowType == PAXOS_MSG_COMMIT {
				return m.Body.Body
			}
			return ""
		}(),
	}
}

func (m *PaCommnMsg) Accept(t *SwapMsgVoteInfo) bool {

	if m.State.HasAccept() {
		return false
	}

	if !t.State.Check(&m.State) {
		panic(fmt.Sprintf("uniq proposeid vote diff t:%+v m:%+v\n", t, m))
	}

	//不接受低的proposid的accept
	if t.State.IsBehind(&m.State) {
		//t的状态需要重置
		//都accept了，不需要关心这个proposeid了把
		t.State.StepToSmallProposeID(m.State.GetProposeId())
		return false
	}

	//accept我不需要关心这个proposeid的值了
	//真正的accept
	return m.State.StepAccept(t.State.Vote)
}

func (m *PaCommnMsg) AcceptAck(t *SwapMsgVoteInfo, membernum uint32) (accept bool) {
	//if !t.Vt.IsAccept() || !m.Vt.IsAccept() {
	//acceptvote = -1
	if !m.State.HasAccept() {
		//对方不会将t的状态进行降级
		//我请求了对方，但是对方认为你的proposeid比较低而且不是accept的状态，可能还是处在propose的状态，但是我已经accept了，不需要你，拒绝了你
		panic(fmt.Sprintf("cur id:%+v invalid accept:%+v", m, t))
	}

	if m.State.HasCommit() {
		if m.State.Check(&t.State) {
			panic(fmt.Sprintf("seq:%d m:%+v \n t:%+v\n", m.GetSeqID(), m, t))
		}
		return
	}

	if t.State.HasCommit() {
		//直接拷贝吧，还想啥
		m.State = t.State
		//清理一下当前的存储
		accept = true
		return
	}

	//时刻保持最大proposeid
	//我已经accept了，没有必要再更新最大的proposeid了
	//bBigger := m.Vt.UpdateProposeid(&t.Vt)
	//accept的时候，已经不需要考虑proposeid了
	//没有accept状态的这种也是要放入的，commit后就不用管了
	st := m.AcceptRes.AddAndJudge(t.State.GetVote(), membernum)
	if st.HasAccept() {
		accept = true
		//将当前的值进行commit
		m.State.StepCommit(m.FromId)
	}
	//要检查是不是肯定不能成功了，检查所有的票状态
	//感知到如果是空洞的话，这里提交的就失败了
	return
}

func IsInValidVote(vote uint32) bool {
	return vote == math.MaxUint32
}

func (m *PaCommnMsg) Commit(t *SwapMsgVoteInfo) bool {
	if !t.State.HasCommit() {
		panic(fmt.Sprintf("no commit %+v %+v", m, t))
	}

	if m.State.HasCommit() {
		if !m.State.Check(&t.State) {
			panic(fmt.Sprintf("t:%+v m:%+v", t, m))
		}
		//不允许修改
		return false
	}
	//有可能有的地方没有accept
	m.State = t.State
	return true
}
