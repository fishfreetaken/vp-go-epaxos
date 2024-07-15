package paxoscommm

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type PaNode struct {
	id uint32 //节点id

	priority uint32 //保留字段吧，优先级

	//每一个提交都建立一个唯一的事件id，这个id还是需要放到
	instanceid uint64
	//seqmap     map[int64]int64 //我要知道哪些是我自己的提出来的 ,key to instanceid

	incresultcnt uint64
	//当前的
	curseq uint64

	commitMsgCommit chan uint64

	g *PaGroup

	//这个可以通过vector来进行切分
	dict sync.Map //[int64]*PaCommnMsg

	//这个只是用来进行保存本地提交的seq, 这个用instacneid to seqid key来触发处理
	// sync map还是防止并发
	mpLocPropose sync.Map //map[int64]*AckState

	vecChans []chan *SwapMsgVoteInfo

	msgChannel chan *ClientReq
}

func (m *PaNode) SetVecLkNums(g *PaGroup, membernum uint32) {
	if membernum < 3 {
		panic(fmt.Sprintf("invalid member num:%d", membernum))
	}

	//初始化一个通道
	m.msgChannel = make(chan *ClientReq, 500*membernum)
	go m.ListNewMsg()

	channum := membernum * 21
	m.vecChans = make([]chan *SwapMsgVoteInfo, channum)

	for i := uint32(0); i < channum; i++ {
		m.vecChans[i] = make(chan *SwapMsgVoteInfo, channum*80)
	}

	fmt.Printf("veclock init vecchans len:%d vecchans len:%d  real chan len:%d msgchan len:%d\n", len(m.vecChans), len(m.vecChans), len(m.vecChans[0]), channum*80)
	for i := 0; i < int(channum); i++ {
		go m.Step2(i)
	}

	m.g = g
}

func (m *PaNode) BeginNewCommit(r *ClientReq) {
	r.Instanceid = atomic.AddUint64(&m.instanceid, 1)
	m.msgChannel <- r
}

func (m *PaNode) ListNewMsg() {
	for v := range m.msgChannel {
		m.NewProPoseMsg(v)
	}
}

func (m *PaNode) NewProPoseMsg(req *ClientReq) {
	// 获取当前进度的seqid
	iCurInstanceId := req.Instanceid
	if iCurInstanceId == 0 {
		panic(fmt.Sprintf("nodeid:%d zero cur instanceid", m.id))
	}

	var iCurSeq uint64
	var proposeid uint32
	var swapMsg *SwapMsgVoteInfo
	//fmt.Printf("new msg node:%d instacneid:%d\n", m.id, iCurInstanceId)
	for {
		proposeid = m.BuildProposeId()

		//尽量减少中间等待的时间
		iCurSeq = atomic.AddUint64(&m.curseq, 1)
		cm := &PaCommnMsg{
			State: PaxosState{
				Vote:      m.id,
				ProposeId: proposeid,
			},
			Seq:    iCurSeq,
			FromId: m.id,
			Body:   *req,
		}
		_, load := m.dict.LoadOrStore(iCurSeq, cm)
		if !load {
			//fmt.Printf("nodeid:%d begin goto seq:%d to proposeid:%d cm:%+v cm:%p\n", m.id, iCurSeq, proposeid, cm, &cm)
			//这里中间可能会发生改变，这个seq值被其他人占用
			if atomic.LoadUint32(&(cm.State.ProposeId)) <= proposeid {
				//这个值已经被别的抢占小了，重新发起
				swapMsg = cm.BuildSwapMsg(PAXOS_MSG_BEGIN_PROPOSE)
				//fmt.Printf("nodeid:%d has load this seq:%d newCurSeq:%d value:%+v\n", m.id, iCurSeq, atomic.LoadInt64(&m.curseq), cm)
				break
			}
		}
		//这个值已经存在了，重新再找一个点
	}

	m.mpLocPropose.Store(iCurInstanceId, &AckState{
		//t:   time.Now(),
		seq: iCurSeq,
	})

	m.g.Broadcastexcept(swapMsg)
	//fmt.Printf("after nodeid:%d instanceid:%d seqid:%d %d\n", m.id, iCurInstanceId, iCurSeq, m.curseq)

	return
}

type AckState struct {
	//t     time.Time
	seq   uint64
	iCode int //返回的结果 0
}

func (m *PaNode) BuildProposeId() uint32 {
	//这里是保证唯一的，而且如果有竞争的话，相对来说值越大的概率更高
	return uint32(rand.Intn(1000000)*GetPosCount(m.g.GetNumber())) + m.id
}

const (
	PANODE_RESULT_NOP           = iota //失效了
	PANODE_RESULT_SUC           = 1    //成功
	PANODE_RESULT_BIG_PROPOSEID = 2    //更大的proposeid
	PANODE_RESULT_OTHER_ACCEPT  = 3    //其他人已经accept过了
	PANODE_RESULT_IMPOSSIBLE    = 4    //选票被瓜分，不可能成功了
	PANODE_RESULT_COMMIT        = 5    //提交数据
)

func (m *PaNode) AsyncWork(wg *sync.WaitGroup) {
	if atomic.LoadUint64(&m.instanceid) == 0 {
		return
	}
	wg.Add(1)
	go m.AsyncReportAndRetry(wg)
}

func (m *PaNode) AsyncReportAndRetry(wg *sync.WaitGroup) {
	//设置一个15秒的超时时间
	t := time.NewTicker(time.Second * 40)

	tsub := time.NewTicker(time.Second)

	defer func() {
		fmt.Printf("node id:%d has done\n", m.id)
		wg.Done()
		t.Stop()
		tsub.Stop()
	}()
	//本地如果没有提交的值可以不用等待
	for {
		select {
		//改成间隔一小段时间来轮训了
		case <-tsub.C:
			//都1️以最终的衡量
			bAllPass := true
			func() {
				istancenum := atomic.LoadUint64(&m.instanceid)
				recvCnt := atomic.LoadUint64(&m.incresultcnt)
				if recvCnt < istancenum {
					//避免全量轮训耗时增加
					bAllPass = false
					return
				} else if recvCnt > istancenum {
					panic(fmt.Sprintf("recvCnt:%d mp:%d", recvCnt, istancenum))
				}
				fmt.Printf("[Trace]AsyncReportAndRetry node:%d recvCnt:%d instannum:%d\n", m.id, recvCnt, istancenum)
				m.mpLocPropose.Range(func(key, value interface{}) bool {
					st := value.(*AckState)
					if st.iCode == PANODE_RESULT_NOP {
						bAllPass = false
						return false
					}
					return true
				})
			}()
			if bAllPass {
				fmt.Printf("AsyncReportAndRetry suc new all pass id:%d retry \n", m.id)
				return
			}
		case <-t.C:
			var noDecideList []uint64
			func() {
				m.mpLocPropose.Range(func(key, value interface{}) bool {
					instanceid := key.(uint64)
					v := value.(*AckState)
					if v.iCode == 0 {
						noDecideList = append(noDecideList, instanceid)
						fmt.Printf("no decide msg detail nodeid:%d instanceid :%d seq:%d msgdetail:%+v\n", m.id, instanceid, v.seq, m.GetSeqMsg(v.seq))
					}
					return true
				})
			}()
			fmt.Printf("AsyncReportAndRetry timeout nodeid:%d nodecidelist:%+v\n", m.id, noDecideList)
			return
		}
	}
}

func (m *PaNode) CalcLastReport() string {
	//warn 这里不需要并发，最后才请求的
	var cntsuc int
	var cnt2 int
	var cnt3 int
	var cnt4 int
	var cntimpossible int

	m.mpLocPropose.Range(func(key, value interface{}) bool {
		instanceid := key.(uint64)
		vt := value.(*AckState)
		//查看每一个inst是否满足ok
		v, ok := m.dict.Load(vt.seq)
		if !ok {
			//这种情况不可能呀
			panic(fmt.Sprintf("nodeid:%d seqid:%d not load ", m.id, vt.seq))
		}
		pMsg := v.(*PaCommnMsg)
		//
		if vt.iCode == PANODE_RESULT_BIG_PROPOSEID {
			cnt2++
		} else if vt.iCode == PANODE_RESULT_OTHER_ACCEPT || vt.iCode == PANODE_RESULT_COMMIT {
			cnt3++
		} else if vt.iCode == PANODE_RESULT_IMPOSSIBLE {
			cnt4++
		} else if vt.iCode == PANODE_RESULT_SUC {
			cntsuc++
			result, exist := m.g.mpResult[pMsg.GetSeqID()]
			if !pMsg.State.HasAccept() || !exist || result != m.id {
				panic(fmt.Sprintf("nodid:%d ins:%d seq:%d exist:%v code:%d result:%d msg:%+v", m.id, instanceid, vt.seq, exist, vt.iCode, m.g.mpResult[pMsg.GetSeqID()], pMsg))
			}
		} else {
			//说明这个提议没有被推进，也是一种异常的情况
			fmt.Printf("invalid stata nodeid:%d seq:%d ins:%d code:%d t:%+v\n", m.id, vt.seq, instanceid, vt.iCode, pMsg)
			cntimpossible++
		}
		return true
	})

	//if m.id == 5 {
	for idx, v := range m.vecChans {
		if len(v) > 0 {
			fmt.Sprintf("[WARNING]CalcLastReport not consume idx:%d len:%d\n", idx, len(v))
		}
	}
	//}

	return fmt.Sprintf("CalcLastReport [nodeid:%d][total:%d][suc:%d][bigpropose:%d][other_accept:%d][impossible:%d][other_case:%d]", m.id, m.instanceid, cntsuc, cnt2, cnt3, cnt4, cntimpossible)
}

//异步发起进行重试
func (m *PaNode) ResultReport(r *PaCommnMsg, result int) (res int32) {
	//check
	iLocalInstancId := r.Body.Instanceid
	if iLocalInstancId == 0 {
		//本地没有注册这个值
		res = -1
		return
	}

	//索引对应的seq
	rawValue, ok := m.mpLocPropose.Load(iLocalInstancId)
	if !ok {
		//有可能是这种情况，我还没有来得及注册，已经通知到我了，直接返回这个值，通过后边自身的学习去感知这个值
		//fmt.Printf("[Warning]ResultReport not exist node:%d ins:%d result:%d msg:%+v \n", m.id, r.InstanceId, result, r)
		res = -1
		return
	}
	value := rawValue.(*AckState)
	if value.seq != r.Seq {
		panic(fmt.Sprintf("[Error]ResultReporr value not equal node:%d ins:%+v result:%d msg:%+v ", m.id, value, result, r))
	}
	if value.iCode != 0 {

		//fmt.Printf("[warning]ResultReport not equal nodeid:%d instance id :%d valueseq:%d r seq:%d  resultcode:%d res:%d\n", m.id, iCurInstance, value.seq, r.Vt.Seq, value.iCode, result)
		//insance所对应的seq已经发生变化，不需要进行处理
		res = -3
		return
	}
	value.iCode = result

	if result == PANODE_RESULT_SUC {
		if !(r.State.GetVote() == m.id) {
			panic(fmt.Sprintf("node:%d instacnid:%d invalid msg:%+v", m.id, iLocalInstancId, r))
		}
		m.g.InformVoteResult(r.BuildSwapMsg(PAXOS_MSG_COMMIT))
		//成功了就需要通知下游按照seq的顺序来执行了
	} else {
		if r.State.HasAccept() && r.State.IsVote(m.id) {
			panic(fmt.Sprintf("node:%d instacnid:%d result:%d  invalid msg:%+v", m.id, iLocalInstancId, result, r))
		}
		//fmt.Printf("ResultReport node:%d instacnid:%d result:%d value:%+v invalid msg:%+v\n", m.id, r.InstanceId, result, value, v.(*PaCommnMsg))
		//return -1

		//fmt.Printf("ResultReport need retry id:%d seqid:%d instanc:%d resultCode:%d  msg:%+v\n", m.id, r.Vt.Seq, iCurInstance, result, r)
		//如果失败了就要发起重试
		if r.Body.RetryTimes <= 0 {
			fmt.Printf("[Warning][nodeid:%d][instanceid:%d]has exceed retrytimes ", m.id, iLocalInstancId)
		} else {
			r.Body.RetryTimes--
			//最后ok了再重试
			//m.BeginNewCommit(&r.Body)
		}
	}

	//只有正常了才能回传信息，顺便清空下数值
	atomic.AddUint64(&m.incresultcnt, 1)
	if r.State.IsCommit() {
		//很有可能阻塞大盘，可以考虑支持异步的go进行操作
		//m.commitMsgCommit <- r.GetSeqID()
	}

	return 0
}

func (m *PaNode) Step2(idx int) {
	for msg := range m.vecChans[idx] {
		//这里区分渠道进行处理，减少go的产生
		m.Step(msg)
	}
}

func (m *PaNode) Step(t *SwapMsgVoteInfo) {
	//fmt.Printf("step except i:%d from:%d seq:%d flowtye:%d\n  ", m.id, t.Vt.FromId, t.GetSeqID(), t.Flowtype)
	//每次要更新最大的seq值
	for {
		tmpseq := atomic.LoadUint64(&m.curseq)
		if tmpseq < t.GetSeqID() {
			//更新最大的seq
			if atomic.CompareAndSwapUint64(&m.curseq, tmpseq, t.GetSeqID()) {
				break
			}
		} else {
			break
		}
	}

	r := m.GetSeqMsg(t.GetSeqID())

	//基本检查
	if !r.State.Check(&t.State) {
		panic(fmt.Sprintf("node:%d m:%+v \n t:%+v\n", m.GetId(), r, t))
	}

	//减少锁的力度
	switch t.Flowtype {
	case PAXOS_MSG_PROPOSE:
		m.Propose(t, r)
		go m.g.Sendto(t.StepAck(m.id), t)
	case PAXOS_MSG_PROPOSE_ACK:
		m.ProposeAck(t, r)
	case PAXOS_MSG_ACCEPT:
		if m.Accept(t, r) {
			go m.g.Sendto(t.StepAck(m.id), t)
		}
	case PAXOS_MSG_ACCEPT_ACK:
		m.AcceptAck(t, r)
	case PAXOS_MSG_COMMIT:
		m.Commit(t, r)
	default:
		panic(fmt.Sprintf("nodeid:%d invalid flow type t:%+v m:%+v", m.id, t, r))
	}
}

//传指针
func (m *PaNode) Propose(t *SwapMsgVoteInfo, r *PaCommnMsg) {
	//提议阶段InForm
	//做一件很简单的事情，比较proposeid，拷贝赋值
	r.Propose(t)
	//这个肯定是要ack的
	//能减少copy新的就用老的就好了吧
}

func (m *PaNode) ProposeAck(t *SwapMsgVoteInfo, r *PaCommnMsg) {
	/*
		if r.State.HasCommit() {
			//分析一下这里不能的根本原因
			//补充一个，如果自己预先已经commit了，将这个结果补充进去(千万不要以为自己想通了就删掉,之前删除过一次，遇到阻塞又加回来了)
			//这里已经commit了，为什么不在commit的地方进行通知呢
			m.ResultReport(r, PANODE_RESULT_COMMIT)
			return
		}
	*/
	if !r.State.IsPropose() || !r.State.IsVote(m.id) {
		//状态已经发生变化了，没有必要接受后边的请求
		//我已经accept了，没有意义接受这个阶段
		//减少cpu压力，及时返回，减少无意义的判断释
		return
	}

	//t有可能是commit的
	if t.State.HasCommit() {
		m.Commit(t, r)
		return
	} else if t.State.HasAccept() && !t.State.IsBehind(&r.State) {
		m.Accept(t, r)
		return
	}

	//正常的提交流程
	r.ProposeAck(t, m.g.GetNumber())
	if !r.State.IsPropose() {
		if !r.State.IsVote(m.id) {
			//选的不是自己的就直接失败了
			//或者被其他的accept住了
			m.ResultReport(r, PANODE_RESULT_BIG_PROPOSEID)
		} else if r.State.HasAccept() {
			go m.g.Broadcastexcept(r.BuildSwapMsg(PAXOS_MSG_ACCEPT))
		} else {
			panic(fmt.Sprintf("impossible isu:%+v r:%+v", t, r))
		}
	}
}

func (m *PaNode) Accept(t *SwapMsgVoteInfo, r *PaCommnMsg) (bAck bool) {
	if !t.State.IsAccept() {
		//你不应该承受这个消息的
		panic(fmt.Sprintf("[accept] invalid msg t:%+v r:%+v has commit", t))
	}

	if t.State.HasCommit() {
		m.Commit(t, r)
		return
	}
	bAck = true
	if r.Accept(t) {
		if !r.State.IsVote(m.id) {
			//这里智能接受别人给的值，选举自己的就肯定有问题
			m.ResultReport(r, PANODE_RESULT_OTHER_ACCEPT)
		}
		//fmt.Printf("[DEBUG]Accept valid id:%d t:%+v r:%+v \n", m.id, t, r)
		//自己接受了，但是不是自己的值，这里其实已经失败了
	}
	return
}

func (m *PaNode) AcceptAck(t *SwapMsgVoteInfo, r *PaCommnMsg) {
	//将数据写入本地的msg里
	bAccept := r.AcceptAck(t, m.g.GetNumber()) // 返回是否落盘
	//判断是否能进入到下一个阶段
	if bAccept {
		if r.State.HasCommit() {
			if r.State.IsVote(m.id) {
				m.ResultReport(r, PANODE_RESULT_SUC)
			} else {
				m.ResultReport(r, PANODE_RESULT_IMPOSSIBLE)
			}
			swapmsg := r.BuildSwapMsg(PAXOS_MSG_COMMIT)

			//有可能所有人不可能达到最后的结果
			go m.g.Broadcastexcept(swapmsg)
			//有可能别人帮他提交的，允许这种情况出现
			//减少通信的次数 ，统计到不可能的情况也需要告诉别人结果
		} else {
			m.ResultReport(r, PANODE_RESULT_IMPOSSIBLE)
		}
		//fmt.Printf("[TRACE][AcceptAck]commit done id:%d seq:%d acceptVoted:%d vt:%+v  r:%+v \n", m.id, t.Vt.Seq, acceptVoted, t, r)
	}
}

func (m *PaNode) GetSeqMsg(seq uint64) *PaCommnMsg {
	v, exist := m.dict.Load(seq)
	if exist {
		return v.(*PaCommnMsg)
	}
	//创建一个新的msg
	var msgbody = PaCommnMsg{
		Seq:    seq,
		FromId: m.id,
	}
	//有可能捞到自己提交的值
	actMsgBody, _ := m.dict.LoadOrStore(seq, &msgbody)
	return actMsgBody.(*PaCommnMsg)
}

func (m *PaNode) GetId() uint32 {
	return m.id
}

func (m *PaNode) Recv(t SwapMsgVoteInfo) {
	go m.Step(&t)
}

func (m *PaNode) Recv2(t SwapMsgVoteInfo) {
	m.vecChans[int(t.GetSeqID())%len(m.vecChans)] <- &t
}

func (m *PaNode) Commit(t *SwapMsgVoteInfo, r *PaCommnMsg) {
	if r.Commit(t) {
		//不管谁都通知一下吧
		if r.State.IsCommit() && r.State.IsVote(m.id) {
			m.ResultReport(r, PANODE_RESULT_SUC)
		} else {
			m.ResultReport(r, PANODE_RESULT_COMMIT)
		}
	}
}

type FuncManageReqMsg func(string)

//只有最终commit的话，才会去拉取对应的消息
func (m *PaNode) AsyncManageCommit(f FuncManageReqMsg) {
	var iCurManageInstance uint64
	for iIns := range m.commitMsgCommit {
		for i := iCurManageInstance + 1; i <= iIns; i++ {
			iCurManageInstance++
			//拉取ins对应的msg
			r := m.GetSeqMsg(iCurManageInstance)
			f(r.Body.Body)
		}
	}
	//拿到锁的话就发一条通知
}
