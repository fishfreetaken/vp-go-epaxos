package paxoscommm

import (
	"bytes"
	"fmt"
	"math"
	"runtime"
)

const (
	PAXOS_MSG_BEGIN_PROPOSE   = iota //正在发起propose的流程中
	PAXOS_MSG_SMALL_PROPOSEID        //拒绝的状态
	PAXOS_MSG_HAS_ACCEPTED           //已经接受了，这里是确定提议
	PAXOS_MSG_ACCEPTED_FAILED        //accept我自己失败了，因为我的proposeid比你的大，但是你已经accept了我没有办法改变
	PAXOS_MSG_HAS_COMMITED           //已经提交，有一些节点可能被其他的accept，所以需要commit阶段进行提交
	PAXOS_MSG_HAS_FAILED             //这次决议已经失败，选票被瓜分了，没有谁获得了成功过
)

type PaxosState struct {
	State     int32
	ProposeId uint32 //这个值是要比较和更新最大的
	Vote      uint32 //选的谁
}

func (m *PaxosState) GetVote() uint32 {
	return m.Vote
}

func (m *PaxosState) IsVote(v uint32) bool {
	return m.Vote == v
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
func (m *PaxosState) StepToSmallProposeID(t *PaxosState) {
	m.State = PAXOS_MSG_SMALL_PROPOSEID
	m.ProposeId = t.ProposeId
	m.Vote = t.Vote
}

func (m *PaxosState) StepAccept(vote uint32) bool {
	if m.HasAccept() {
		return false
	}
	m.State = PAXOS_MSG_HAS_ACCEPTED
	m.Vote = vote
	return true
}

func (m *PaxosState) StepAcceptFailed() bool {
	if m.HasCommit() {
		return false
	}
	m.State = PAXOS_MSG_ACCEPTED_FAILED
	return true
}

func (m *PaxosState) IsAcceptFailed() bool {

	return m.State == PAXOS_MSG_ACCEPTED_FAILED
}

func (m *PaxosState) StepCommit(vote uint32) bool {
	if m.HasCommit() {
		return false
	}
	m.State = PAXOS_MSG_HAS_COMMITED
	m.Vote = vote
	return true
}

func (m *PaxosState) printStack() {
	var buf bytes.Buffer

	// 获取当前的堆栈信息
	stack := make([]byte, 1024)
	length := runtime.Stack(stack, false)

	// 打印堆栈信息
	buf.Write(stack[:length])
	fmt.Printf("m:%+v stack:%s ", m, buf.String())
}

//commit failed
func (m *PaxosState) StepFailed() bool {
	if m.IsCommit() {
		panic(fmt.Sprintf("step failed has commit:%+v", m))
	}
	if m.IsFailed() {
		return false
	}

	//m.printStack()
	m.State = PAXOS_MSG_HAS_FAILED
	m.Vote = math.MaxUint32
	return true
}

//定义proposeid落后的语意
func (m *PaxosState) IsBehind(p *PaxosState) bool {
	return p.ProposeId > m.ProposeId
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
	Seq      uint64
	State    PaxosState //每个位置的状态也不一致
	Fromid   uint32
	Cmd      string //有可能带上执行的命令
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

func isPaxosFail(failcnt, membernum uint32) bool {
	return failcnt >= ((membernum >> 1) + (membernum % 2))
}

func (m *ProposeInfo) Judge(st *PaxosState, membernum uint32) {
	if st.HasAccept() {
		panic(fmt.Sprintf("st:%+v has accept proposinfo:%+v", st, m))
	}
	passNum := membernum >> 1

	if isPaxosFail(m.failCnt, membernum) {
		st.StepAcceptFailed()
		fmt.Printf("[Warning]ProposeInfo judge id:%d member:%d faile:%d suc:%d has impossbile\n", st.GetVote(), membernum, m.failCnt, m.sucCnt)
		return
	}
	if m.sucCnt >= passNum {
		st.StepAccept(st.GetVote())
		return
	}
	return
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
	if t.State.HasAccept() {
		panic(fmt.Sprintf("invalid type m:%+v t:%+v", m, t))
	}

	if !m.State.HasAccept() && m.State.IsBehind(&t.State) {
		m.State = t.State
		return
	}
	//已经accept，没有这个简洁完美的版本，ack还是需要知道m的当前状态的
	t.State = m.State
}

func (m *PaCommnMsg) ProposeAck(t *SwapMsgVoteInfo, membersNum uint32) {
	//避免自己收到自己消息的情况
	if m.FromId == t.GetFromID() {
		panic(fmt.Sprintf("cannot vote myself fromid:%d t:%+v m:%+v", m.GetFromID(), t, m))
	}

	if t.State.HasAccept() {
		if t.State.IsBehind(&m.State) {
			//这个时候我还有可能胜利的
			m.ProposeRes.IncFail()
		} else {
			//这里要走后边的accept的流程，这个其实用户已经进入accept了，这里其实不可能走到这里
			panic(fmt.Sprintf("accept can input t:%+v m:%+v", t, m))
		}
	} else {
		if t.State.IsBehind(&m.State) {
			//老的无效数据不考虑了
			//特别异常的一个情况，只有冲突的时候才会有这种情况
			// 我更新了一个最大的proposeid，但是后边又来了一个旧值，所以这种情况还是允许发生的
			//panic(fmt.Sprintf("smaller proporse than local t:%+v m:%+v\n", t, m))
			return
		} else if m.State.IsBehind(&t.State) {
			//对方的ProposeId比自己的大 ，todo 放弃或者择机重新发起
			// 三个策略：
			// 1 冲突放弃；2 冲突重试；3 冲突接受(算是我学习到了这个值)
			//fmt.Printf("bigger proporse than local seq:%d  remote %d:%d_local:%d:%d\n", t.Vt.Seq, t.Vt.FromId, t.Vt.ProposeId, m.Vt.FromId, m.Vt.ProposeId)
			//先接受这个提议，挡住后边的accept请求,,如果收到醉倒的proposeid的话，一定要更新proposeid
			//m.Vt.ProposeId = t.Vt.ProposeId
			//fmt.Printf("SetPropose small before nodeid:%d t:%+v m:%+v\n", m.FromId, t, m)
			//遇到小的值退出
			//这里其实是我的proposid小了，要接受你的值
			m.State.StepToSmallProposeID(&t.State)
			//这里也是要触发一个新的提交流程
			//fmt.Printf(" localnodid:%d update t:%+v m:%+v\n", nodeid, t, m)
			return
		}
		m.ProposeRes.IncSuc()
	}

	//看你的选票还是我的话，可以继续完
	if !m.State.IsVote(m.FromId) {
		panic(fmt.Sprintf("impossible accept you t:%+v m:%+v", t, m))
	}

	m.ProposeRes.Judge(&m.State, membersNum)
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
	if t.State.HasCommit() {
		panic(fmt.Sprintf("uniq proposeid vote diff t:%+v m:%+v\n", t, m))
	}

	if m.State.HasAccept() {
		//这里还是要处理拷贝的，这了是别人已经accept了
		t.State = m.State
		return false
	}

	//不接受低的proposid的accept，我没有accept，我就要拿你的值来替换我的
	if t.State.IsBehind(&m.State) {
		//t的状态需要重置，这里已经impossible了
		//都accept了，不需要关心这个proposeid了把
		t.State.StepAcceptFailed()
		return false
	}

	//accept我不需要关心这个proposeid的值了
	//真正的accept
	if !m.State.StepAccept(t.State.Vote) {
		panic(fmt.Sprintf("step accept failed t:%+v m:%+v\n", t, m))
	}
	return true
}

func (m *PaCommnMsg) AcceptAck(t *SwapMsgVoteInfo, membernum uint32) (change bool) {

	if !m.State.HasAccept() {
		//对方不会将t的状态进行降级
		//我请求了对方，但是对方认为你的proposeid比较低而且不是accept的状态，可能还是处在propose的状态，但是我已经accept了，不需要你，拒绝了你
		panic(fmt.Sprintf("cur id:%+v invalid accept:%+v", m, t))
	}

	//这两个状态已经可以直接返回了
	if m.State.HasCommit() || m.State.IsAcceptFailed() {
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
