package paxoscommm

import (
	"fmt"
	"math/rand"
)

const (
	PAXOS_MSG_BEGIN_PROPOSE = iota //正在发起propose的流程中
	PAXOS_MSG_HAS_ACCEPTED         //已经接受了，这里是确定提议
	PAXOS_MSG_HAS_COMMITED         //已经提交，有一些节点可能被其他的accept，所以需要commit阶段进行提交
)

const (
	PAXOS_OFFSET_ID_SET = 1000
)

type PaxosProposeId struct {
	id uint32
}

func (m *PaxosProposeId) GetNode() uint32 {
	return m.id % PAXOS_OFFSET_ID_SET
}

func (m *PaxosProposeId) IsBehind(t *PaxosProposeId) bool {
	return m.id < t.id
}

func (m *PaxosProposeId) IsEqual(t *PaxosProposeId) bool {
	return m.id == t.id
}

func (m *PaxosProposeId) GenPPId(nodeid uint32) {
	m.id = uint32(rand.Intn(100000)*PAXOS_OFFSET_ID_SET) + nodeid
}

type PaxosMsg struct {
	from uint32
	body []byte
}

func (m *PaxosMsg) GetFrom() uint32 {
	return m.from
}

type PaxosState struct {
	state     int32
	proposeId PaxosProposeId
	acceptmsg PaxosMsg //接受并处理的消息
}

func (m *PaxosState) GetAcceptWho() uint32 {
	return m.acceptmsg.from
}

func (m *PaxosState) IsBigger(s *PaxosState) bool {
	return m.proposeId.IsBehind(&s.proposeId)
}

func (m *PaxosState) IsSameProposeId(s *PaxosState) bool {

	return m.proposeId.id == s.proposeId.id
}

func (m *PaxosState) IsPropose() bool {
	return m.state == PAXOS_MSG_BEGIN_PROPOSE
}

func (m *PaxosState) HasAccept() bool {
	return m.state >= PAXOS_MSG_HAS_ACCEPTED
}

func (m *PaxosState) IsAccept() bool {
	return m.state == PAXOS_MSG_HAS_ACCEPTED
}

func (m *PaxosState) IsCommit() bool {
	return m.state == PAXOS_MSG_HAS_COMMITED
}

func (m *PaxosState) StepAccept() bool {
	if m.HasAccept() {
		return false
	}
	m.state = PAXOS_MSG_HAS_ACCEPTED
	return true
}

func (m *PaxosState) StepCommit() bool {

	if m.IsCommit() {
		return false
	}
	m.state = PAXOS_MSG_HAS_COMMITED
	return true
}

// Flowtype
const (
	PAXOS_MSG_PROPOSE = iota
	PAXOS_MSG_PROPOSE_ACK
	PAXOS_MSG_ACCEPT
	PAXOS_MSG_ACCEPT_ACK
	PAXOS_MSG_COMMIT //不需要ack
)

// 交换消息
type SwapMsgVoteInfo struct {
	Flowtype int32      //最高位置进行标记，节省内存
	state    PaxosState //每个位置的状态也不一致
	seq      uint64
	fromid   uint32
}

func (m *SwapMsgVoteInfo) GetSeqID() uint64 {
	return m.seq
}

func (m *SwapMsgVoteInfo) GetFromId() uint32 {
	return m.fromid
}

func (m *SwapMsgVoteInfo) SetFromId(f uint32) {
	m.fromid = f
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
	Instanceid uint64 //通过这个id索引找的提交的值，看那个提交的值没有通过
	//RetryTimes uint32 //只能进行有限次数的重试吧，不能一直重试，如果多次都被强占了
	pm PaxosMsg
}

type ProposeInfo struct {
	b  PaBitCount
	ac PaxosState //已经接受的最大的请求的值
}

func (m *ProposeInfo) IsSucc(member uint32) bool {
	return m.b.ExceedHalf(member)
}

func (m *ProposeInfo) AddOne(member uint32) bool {
	return m.b.Add(member)
}

type AcceptInfo struct {
	b PaBitCount
}

func (m *AcceptInfo) addOne(from uint32) bool {
	return m.b.Add(from)
}

func (m *AcceptInfo) AcceptSuc(membernum uint32) (c bool) {
	return m.b.ExceedHalf(membernum)
}

type PaCommnMsg struct {
	ps PaxosState //propose value

	ver uint32 //如果不一致，需要计算

	proposeRes ProposeInfo
	acceptRes  AcceptInfo

	body ClientReq //本地提交的时候会保存这个msg
}

func (m *PaCommnMsg) makeChange() {
	m.ver++
}

// 是否已经改变
func (m *PaCommnMsg) HasChange(ver uint32) bool {
	return m.ver < ver
}

func (m *PaCommnMsg) GetVer() uint32 {
	return m.ver
}

// 接受一个提议
func (m *PaCommnMsg) Propose(t *SwapMsgVoteInfo) (ack bool) {

	if t.state.proposeId.IsEqual(&m.ps.proposeId) || t.state.IsBigger(&m.ps) {
		//不需要回复了，这里有可能是数据丢失的
		return
	}

	ack = true
	if m.ps.HasAccept() {
		//告诉提案的请求方，已经接受的值
		//或者拒绝更低的提议
		t.state = m.ps
		return
	}

	//承诺不接受更小的值
	//只更新这里的proposeid
	if m.ps.proposeId.IsBehind(&t.state.proposeId) {
		m.ps.proposeId = t.state.proposeId
		m.makeChange()
	}
	return
}

func (m *PaCommnMsg) ProposeAck(t *SwapMsgVoteInfo, membersNum uint32) (ac *SwapMsgVoteInfo) {

	//假如我的propose超过半数了，中间没有更大的accept的值，我就推举决定我自己
	//假如我propose过程中，收到别人accept过的值，我找一个最大proposeid的来进行accept
	if !m.proposeRes.AddOne(t.GetFromId()) {
		fmt.Sprintf("[Error]ProposeAck duplicate msg t:%+v m:%+v", t, m)
		return
	}

	if t.state.IsAccept() {
		if !m.proposeRes.ac.HasAccept() || m.proposeRes.ac.IsBigger(&t.state) {
			m.proposeRes.ac = t.state
		}
		return
	}

	//过半数后发起accept的流程
	if !m.proposeRes.IsSucc(membersNum) {
		return
	}

	m.makeChange()

	if m.proposeRes.ac.IsAccept() {
		m.ps = m.proposeRes.ac
	} else {
		m.ps.StepAccept()
	}

	//如果是自己提出来的值，直接接受自己
	ac = &SwapMsgVoteInfo{
		Flowtype: PAXOS_MSG_ACCEPT,
		seq:      t.seq,
		state:    m.ps,
	}
	return
}

func (m *PaCommnMsg) Accept(t *SwapMsgVoteInfo) (ack bool) {

	if t.state.IsBigger(&m.ps) {
		//保证不要别人的最小值，不需要返回给对方数据
		return
	}

	//accept的状态我是否需要继续更新这里的值？
	//如果接受到一个更大的值，说明这个值已经占领过半数了，肯定要更新的

	ack = true
	if m.ps.IsCommit() {
		t.state = m.ps
		return
	}

	//如果不一致就要更新
	if !m.ps.IsSameProposeId(&t.state) {
		m.ps = t.state
		m.makeChange()
	}

	return
}

func (m *PaCommnMsg) AcceptAck(t *SwapMsgVoteInfo, membernum uint32) bool {

	if !m.ps.HasAccept() {
		//对方不会将t的状态进行降级
		//我请求了对方，但是对方认为你的proposeid比较低而且不是accept的状态，可能还是处在propose的状态，但是我已经accept了，不需要你，拒绝了你
		panic(fmt.Sprintf("cur id:%+v invalid accept:%+v", m, t))
	}

	//已经添加过了
	if !m.acceptRes.addOne(t.fromid) {
		return false
	}

	m.makeChange()

	//假如接受了超过半数的人

	//时刻保持最大proposeid
	//我已经accept了，没有必要再更新最大的proposeid了
	//accept的时候，已经不需要考虑proposeid了
	//没有accept状态的这种也是要放入的，commit后就不用管了

	//要检查是不是肯定不能成功了，检查所有的票状态
	//感知到如果是空洞的话，这里提交的就失败了

	return m.acceptRes.AcceptSuc(membernum)
}

func (m *PaCommnMsg) Commit(t *SwapMsgVoteInfo) bool {
	if !t.state.IsCommit() {
		panic(fmt.Sprintf("no commit %+v %+v", m, t))
	}

	if m.ps.IsCommit() {
		//不允许修改
		fmt.Sprintf("[Trace]m has commit m:%+v t:%+v", m, t)
		return false
	}
	//有可能有的地方没有accept，直接修改状态
	m.ps = t.state
	return true
}

func (m *PaCommnMsg) BuildSwapMsg(seq uint64, st int32, fromid uint32) *SwapMsgVoteInfo {
	var lstate int32
	rst := &SwapMsgVoteInfo{
		Flowtype: st,
		seq:      seq,
		state: PaxosState{
			proposeId: m.ps.proposeId,
		},
		fromid: fromid,
	}

	if st == PAXOS_MSG_COMMIT {
		rst.state.acceptmsg = m.ps.acceptmsg
		lstate = PAXOS_MSG_HAS_COMMITED
	} else if st == PAXOS_MSG_ACCEPT {
		rst.state.acceptmsg = m.ps.acceptmsg
		lstate = PAXOS_MSG_HAS_ACCEPTED
	} else if st == PAXOS_MSG_PROPOSE {
		//这个不需要给到提议的值
		lstate = PAXOS_MSG_BEGIN_PROPOSE
	} else {
		panic(fmt.Sprintf("st:%d fromid:%d invalid build swap msg:%+v", st, fromid, m))
	}
	rst.state.state = lstate
	return rst
}
