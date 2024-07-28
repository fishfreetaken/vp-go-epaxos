package paxoscommm

import (
	"fmt"
	"math"
)

const (
	PAXOS_MSG_BEGIN_PROPOSE = iota //正在发起propose的流程中
	PAXOS_MSG_HAS_ACCEPTED         //已经接受了，这里是确定提议
	PAXOS_MSG_HAS_COMMITED         //已经提交，有一些节点可能被其他的accept，所以需要commit阶段进行提交
)

type PaxosProposeId struct {
	id uint32
}

func (m *PaxosProposeId) GetNode() uint32 {
	return m.id
}

func (m *PaxosProposeId) IsBehind(t *PaxosProposeId) bool {
	return m.id < t.id
}

func (m *PaxosProposeId) UpdateMax(t *PaxosProposeId) {
	if m.id < t.id {
		m.id = t.id
	}
}

type PaxosMsg struct {
	from uint32
	body []uint32
}

type PaxosState struct {
	state     int32
	proposeId PaxosProposeId //这个值是要比较和更新最大的
	tb        PaxosMsg       //需要处理的消息
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

func (m *PaxosState) StepAccept() bool {
	if m.HasAccept() {
		return false
	}
	m.State = PAXOS_MSG_HAS_ACCEPTED
	return true
}

func (m *PaxosState) StepCommit(vote uint32) bool {
	if m.HasCommit() {
		return false
	}
	m.State = PAXOS_MSG_HAS_COMMITED
	return true
}

//定义proposeid落后的语意
func (m *PaxosState) IsBehind(p *PaxosState) bool {
	return m.proposeId.IsBehind(&p.proposeId)
}

func (m *PaxosState) Check(t *PaxosState) bool {

	if t.IsPropose() && m.IsPropose() && m.ProposeId == t.ProposeId && t.Vote != m.Vote {
		return false
	}
	//commit只能有一个提交
	if m.IsCommit() && t.IsCommit() && m.Vote != t.Vote {
		return false
	}
	return true
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
	fromid   uint32
	Seq      uint64
	state    PaxosState //每个位置的状态也不一致
}

func (m *SwapMsgVoteInfo) GetSeqID() uint64 {
	return m.Seq
}

func (m *SwapMsgVoteInfo) GetFromId() uint32 {
	return m.fromid
}

func (m *SwapMsgVoteInfo) StepAck(f uint32) uint32 {
	if m.Flowtype == PAXOS_MSG_PROPOSE {
		m.Flowtype = PAXOS_MSG_PROPOSE_ACK
	} else if m.Flowtype == PAXOS_MSG_ACCEPT {
		m.Flowtype = PAXOS_MSG_ACCEPT_ACK
	} else {
		panic(fmt.Sprintf("invalid flow type:%+v", m))
	}
	rawfrom := m.fromid
	m.fromid = f
	return rawfrom
}

type ClientReq struct {
	Instanceid uint64 //通过这个id索引找的值
	RetryTimes uint32 //只能进行有限次数的重试吧，不能一直重试，如果多次都被强占了
}

func isImpossible(membernum, curCount, maxNum, passNum uint32) bool {
	if (membernum - curCount + maxNum) < passNum {
		return true
	}
	return false
}

type ProposeInfo struct {
	bit []uint32 //统计数量，用来bitcount
}

func (m *ProposeInfo) AddAckState(fromid uint32) bool {

	pos := fromid / 8
	idx := fromid % 8
	for int(pos) < len(m.bit) {
		m.bit = append(m.bit, 0)
	}

	if (m.bit[pos-1] & (1 << idx)) > 0 {
		return false
	}

	m.bit[pos-1] = m.bit[pos-1] & (1 << idx)

	return true
}

func (m *ProposeInfo) ExceedHalf(membernum uint32) bool {
	var cnt uint32
	for _, v := range m.bit {
		cnt += BitCount(v)
	}
	h := membernum >> 1
	if cnt < h {
		return false
	}

	return true
}

type AcceptInfo struct {
	votelist []uint32 //这里存各个节点的选择的列表，这里也是不需要过滤重复的
	failCnt  uint32   //那些由于proposeid小于的返回的fail
}

func (m *AcceptInfo) addOne(s *PaxosState) (sum uint32) {

	defer func() {
		sum = m.failCnt + uint32(len(m.votelist))
	}()

	if s.IsAcceptFailed() {
		m.failCnt++
		return
	}
	m.votelist = append(m.votelist, s.GetVote())
	return
}

func (m *AcceptInfo) AddAndJudge(s *PaxosState, t *PaxosState, membernum uint32) (c bool) {
	cnt := m.addOne(s)

	passNum := membernum>>1 + 1

	if cnt < passNum {
		//减少计算量
		return
	}

	if cnt > membernum || uint32(len(m.votelist)) > membernum {
		panic(fmt.Sprintf("vote:%d invalid member num:%d", s.Vote, cnt))
	}

	if isPaxosFail(m.failCnt, membernum) {
		//至少有一半以上就是失败了，这里没有必要做什么，等别人commit就好了
		//但是要通知自己已经失败了
		c = t.StepAcceptFailed()
		return
	}

	if uint32(len(m.votelist)) == 0 {
		return
	}

	var mpvote = make(map[uint32]uint32) //vote
	var maxNum uint32
	mpvote[t.Vote] = 1
	//这里看选票是否被瓜分了
	for _, v := range m.votelist {
		mpvote[v]++
		tmpvotenum := mpvote[v]
		if tmpvotenum >= passNum {
			c = t.StepCommit(v)
			return
		}
		if tmpvotenum > maxNum {
			maxNum = tmpvotenum
		}
	}

	//判断选票是否被瓜分了
	//如果这里失败达到一定的次数，本次提交失去了地位已经
	//这里应该考虑failcnt的数量
	if isImpossible(membernum, uint32(len(m.votelist))+1, maxNum, passNum) {
		//选票被瓜分了，谁都胜利不出
		c = t.StepFailed()
		fmt.Printf("impossiable accept vote success maxNum:%d membernum:%d cnt:%d fail:%d len:%d t:%+v s:%+v \n", maxNum, membernum, cnt, m.failCnt, len(m.votelist), t, s)
		return
	}
	return
}

type PaCommnMsg struct {
	pv PaxosState //propose value
	av PaxosState //accept value

	ProposeRes ProposeInfo
	AcceptRes  AcceptInfo

	Body ClientReq //本地提交的时候会保存这个msg
}

func (m *PaCommnMsg) ClearStackMsg() {
	m.AcceptRes.votelist = m.AcceptRes.votelist[:0]
}

//接受一个提议
func (m *PaCommnMsg) Propose(t *SwapMsgVoteInfo) {
	//更新proposeid，作出承诺
	m.UpdateMaxProposeID(t)
}

func (m *PaCommnMsg) UpdateMaxProposeID(t *SwapMsgVoteInfo) {
	if m.pv.proposeId.IsBehind(&t.state.proposeId) {
		m.pv.proposeId.UpdateMax(&t.state.proposeId)
	}
}

func (m *PaCommnMsg) ProposeAck(t *SwapMsgVoteInfo, nodeid, membersNum uint32) (ac *SwapMsgVoteInfo) {

	if m.pv.HasAccept() {
		return
	}
	//去重复
	if !m.ProposeRes.AddAckState(t.GetFromId()) {
		fmt.Printf("[Error]duplicate msg t:%+v m:%+v", t, m)
		return
	}

	if t.state.IsCommit() {
		m.Commit(t)
		return nil
	}

	if t.state.IsAccept() {
		//更新最大的accept的值
		if m.av.proposeId.IsBehind(&t.state.proposeId) {
			m.av = t.state
		}
	}

	m.UpdateMaxProposeID(t)

	//查看是否过半数
	if !m.ProposeRes.ExceedHalf(membersNum) {
		return
	}

	//如果当前选的是自己
	//强制让自己接受自己
	if !m.av.IsAccept() && m.pv.proposeId.GetNode() == nodeid {
		m.pv.StepAccept()

		//如果是自己提出来的值，直接接受自己
		ac = &SwapMsgVoteInfo{
			Flowtype: PAXOS_MSG_ACCEPT,
			fromid:   nodeid,
			Seq:      t.Seq,
			state:    m.pv,
		}

		m.Accept(ac)
	}
	return
}

func (m *PaCommnMsg) Accept(t *SwapMsgVoteInfo) bool {

	if t.state.HasCommit() {
		panic(fmt.Sprintf("uniq proposeid vote diff t:%+v m:%+v\n", t, m))
	}

	if t.state.proposeId.IsBehind(&m.pv.proposeId) {
		return false
	}

	m.pv.proposeId.UpdateMax(&t.state.proposeId)
	m.av = t.state

	return false
}

func (m *PaCommnMsg) AcceptAck(t *SwapMsgVoteInfo, membernum uint32) (change bool) {

	if !m.State.HasAccept() {
		//对方不会将t的状态进行降级
		//我请求了对方，但是对方认为你的proposeid比较低而且不是accept的状态，可能还是处在propose的状态，但是我已经accept了，不需要你，拒绝了你
		panic(fmt.Sprintf("cur id:%+v invalid accept:%+v", m, t))
	}

	//这两个状态已经可以直接返回了
	if m.av.HasCommit() {
		return
	}

	if t.State.HasCommit() {
		//直接拷贝吧，还想啥
		m.State = t.State
		change = true
		//清理一下当前的存储
		return
	}

	//时刻保持最大proposeid
	//我已经accept了，没有必要再更新最大的proposeid了
	//bBigger := m.Vt.UpdateProposeid(&t.Vt)
	//accept的时候，已经不需要考虑proposeid了
	//没有accept状态的这种也是要放入的，commit后就不用管了

	//要检查是不是肯定不能成功了，检查所有的票状态
	//感知到如果是空洞的话，这里提交的就失败了

	return m.AcceptRes.AddAndJudge(&t.State, &m.State, membernum)
}

func IsInValidVote(vote uint32) bool {
	return vote == math.MaxUint32
}

func (m *PaCommnMsg) Commit(t *SwapMsgVoteInfo) bool {
	if !t.State.HasCommit() {
		panic(fmt.Sprintf("no commit %+v %+v", m, t))
	}

	if m.State.HasCommit() {
		//不允许修改
		return false
	}
	//有可能有的地方没有accept，直接修改状态
	m.State = t.State
	return true
}
