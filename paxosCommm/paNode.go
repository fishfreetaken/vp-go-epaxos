package paxoscommm

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type PaNode struct {
	id int //节点id

	priority int

	//这个可以通过vector来进行切分
	dict sync.Map //[int64]*PaCommnMsg

	//这个只是用来进行保存本地提交的seq, 这个用instacneid to seqid key来触发处理
	// sync map还是防止并发
	mpLocPropose sync.Map //map[int64]*AckState

	vecChans []chan PaCommnMsg

	msgChannel chan *ClientReq

	//每一个提交都建立一个唯一的事件id，这个id还是需要放到
	instanceid int64
	//seqmap     map[int64]int64 //我要知道哪些是我自己的提出来的 ,key to instanceid

	incresultcnt int64
	//当前的
	curseq int64
	g      *PaGroup

	curmsgstate *ClientReq
}

func (m *PaNode) GetCurMsgState() ClientReq {

	/*
		for idx, v := range m.vecChans {
			if len(v) == 0 || len(v) == cap(v) {
				fmt.Printf("nodeid:%d idx:%d null channel v:%d\n", m.id, idx, len(v))
			}
		}*/
	return *m.curmsgstate
}

func (m *PaNode) BeginNewCommit(r *ClientReq) {
	r.Instanceid = atomic.AddInt64(&m.instanceid, 1)
	r.Step = -1
	m.msgChannel <- r
}

func (m *PaNode) ListNewMsg() {
	for v := range m.msgChannel {
		m.NewProPoseMsg(v)
	}
}

func (m *PaNode) SetVecLkNums(g *PaGroup, membernum int) {

	if membernum < 3 {
		panic(fmt.Sprintf("invalid member num:%d", membernum))
	}

	//初始化一个通道
	m.msgChannel = make(chan *ClientReq, 800*membernum)
	go m.ListNewMsg()

	channum := membernum * 21
	m.vecChans = make([]chan PaCommnMsg, channum)
	for i := 0; i < channum; i++ {
		m.vecChans[i] = make(chan PaCommnMsg, channum*80)
	}

	for i := 0; i < channum; i++ {
		go m.Step2(i)
	}

	//在本地的函数中创建，在外边创建找不到

	//m.seqmap = make(map[int64]int64)
	m.g = g

}

func (m *PaNode) NewProPoseMsg(req *ClientReq) *PaCommnMsg {
	//获取当前进度的seqid
	m.curmsgstate = req
	iCurInstanceId := req.Instanceid
	if iCurInstanceId == 0 {
		panic(fmt.Sprintf("nodeid:%d zero cur instanceid", m.id))
	}

	var iCurSeq int64
	var proposeid int32

	//fmt.Printf("new msg node:%d instacneid:%d\n", m.id, iCurInstanceId)
	for {
		proposeid = m.BuildProposeId()

		//尽量减少中间等待的时间
		iCurSeq = atomic.AddInt64(&m.curseq, 1)
		var cm = PaCommnMsg{
			Vt: VoteInfo{
				ProposeId:   proposeid,
				ProposeVote: m.id,
				Seq:         iCurSeq,
				FromId:      m.id,
			},
			Body: req,
		}
		req.Step = 2
		_, load := m.dict.LoadOrStore(iCurSeq, &cm)
		if !load {
			//fmt.Printf("nodeid:%d begin goto seq:%d to proposeid:%d cm:%+v cm:%p\n", m.id, iCurSeq, proposeid, cm, &cm)
			//这里中间可能会发生改变，这个seq值被其他人占用
			if atomic.LoadInt32(&(cm.Vt.ProposeId)) <= proposeid {
				//这个值已经被别的抢占小了，重新发起
				cm.InstanceId = iCurInstanceId
				req.Step = 3
				//cmmsg = &cm
				//fmt.Printf("nodeid:%d has load this seq:%d newCurSeq:%d value:%+v\n", m.id, iCurSeq, atomic.LoadInt64(&m.curseq), cm)
				break
			}
		}
		//这个值已经存在了，重新再找一个点
	}

	res := &PaCommnMsg{
		Vt: VoteInfo{
			ProposeId:   proposeid,
			ProposeVote: m.id,
			Seq:         iCurSeq,
			FromId:      m.id,
		},
		Body: req,
	}
	//fmt.Printf("[debug]NewProPoseMsg succnode:%d instid:%d seq:%d  cmmsg:%+v\n", m.id, iCurInstanceId, iCurSeq, cmmsg)

	//本地先注册
	req.Step = 4
	//fmt.Printf("before nodeid:%d instanceid:%d seqid:%d %d\n", m.id, iCurInstanceId, iCurSeq, m.curseq)
	m.mpLocPropose.Store(iCurInstanceId, &AckState{
		//t:   time.Now(),
		seq: iCurSeq,
	})

	//fmt.Printf("mid nodeid:%d instanceid:%d seqid:%d %d\n", m.id, iCurInstanceId, iCurSeq, m.curseq)
	//不需要等待
	req.Step = 5
	m.g.Broadcastexcept(*res, m.id)
	//fmt.Printf("after nodeid:%d instanceid:%d seqid:%d %d\n", m.id, iCurInstanceId, iCurSeq, m.curseq)
	req.Step = 6
	return res
}

const (
	PAXOS_MSG_PROPOSE = iota
	PAXOS_MSG_PROPOSE_ACK
	PAXOS_MSG_ACCEPT
	PAXOS_MSG_ACCEPT_ACK
	PAXOS_MSG_COMMIT //不需要ack
)

type AckState struct {
	//t     time.Time
	seq   int64
	iCode int //返回的结果 0
}

func (m *PaNode) BuildProposeId() int32 {
	//这里是保证唯一的，而且如果有竞争的话，相对来说值越大的概率更高
	return int32(rand.Intn(100000)*GetPosCount(m.g.GetNumber()) + m.id)
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
	if atomic.LoadInt64(&m.instanceid) == 0 {
		return
	}
	wg.Add(1)
	go m.AsyncReportAndRetry(wg)
}

func (m *PaNode) AsyncReportAndRetry(wg *sync.WaitGroup) {
	//设置一个15秒的超时时间
	t := time.NewTicker(time.Second * 60)

	tsub := time.NewTicker(time.Millisecond * 1000)

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
				istancenum := atomic.LoadInt64(&m.instanceid)
				recvCnt := atomic.LoadInt64(&m.incresultcnt)
				if recvCnt < istancenum {
					//避免全量轮训耗时增加
					bAllPass = false
					return
				} else if recvCnt > istancenum {
					panic(fmt.Sprintf("recvCnt:%d mp:%d", recvCnt, istancenum))
				}
				m.mpLocPropose.Range(func(key, value interface{}) bool {
					st := value.(*AckState)
					/*
						if v.iCode != PANODE_RESULT_SUC {
							bAllPass = false
						}
					*/
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
			var noDecideList []int64
			func() {
				m.mpLocPropose.Range(func(key, value interface{}) bool {
					instanceid := key.(int64)
					v := value.(*AckState)
					/*
						if v.iCode != PANODE_RESULT_SUC {
							noDecideList = append(noDecideList, key)
						}
					*/
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
		instanceid := key.(int64)
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
			result, exist := m.g.mpResult[pMsg.Vt.Seq]
			if !pMsg.Vt.IsAccept() || !exist || result != m.id {
				panic(fmt.Sprintf("nodid:%d ins:%d seq:%d exist:%v code:%d result:%d msg:%+v", m.id, instanceid, vt.seq, exist, vt.iCode, m.g.mpResult[pMsg.Vt.Seq], pMsg))
			}
		} else {
			//说明这个提议没有被推进，也是一种异常的情况
			fmt.Printf("invalid stata nodeid:%d seq:%d ins:%d code:%d t:%+v\n", m.id, vt.seq, instanceid, vt.iCode, pMsg)
			cntimpossible++
		}
		return true
	})
	return fmt.Sprintf("CalcLastReport [nodeid:%d][total:%d][suc:%d][bigpropose:%d][other_accept:%d][impossible:%d][other_case:%d]", m.id, m.instanceid, cntsuc, cnt2, cnt3, cnt4, cntimpossible)
}

//异步发起进行重试
func (m *PaNode) ResultReport(r *PaCommnMsg, result int) (res int32) {
	//check
	/*
		v, ok := m.dict.Load(r.Vt.Seq)
		if !ok {
			panic(fmt.Sprintf("node:%d instacnid:%d invalid msg:%+v", m.id, iCurInstance, v.(*PaCommnMsg)))
		}
	*/
	if r.InstanceId == 0 {
		//本地没有注册这个值
		res = -1
		return
	}

	defer func() {
		if res == 0 {
			//只有正常了才能回传信息，顺便清空下数值
			r.ClearStackMsg("")
			atomic.AddInt64(&m.incresultcnt, 1)
		}
	}()

	//索引对应的seq
	rawValue, ok := m.mpLocPropose.Load(r.InstanceId)
	if !ok {
		//有可能是这种情况，我还没有来得及注册，已经通知到我了，直接返回这个值，通过后边自身的学习去感知这个值
		//fmt.Printf("[Warning]ResultReport not exist node:%d ins:%d result:%d msg:%+v \n", m.id, r.InstanceId, result, r)
		res = -1
		return
	}
	value := rawValue.(*AckState)
	if value.seq != r.Vt.Seq {
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
		if r.Vt.AcceptVote != m.id {
			panic(fmt.Sprintf("node:%d instacnid:%d invalid msg:%+v", m.id, r.InstanceId, r))
		}
	} else {
		if r.Vt.IsCommit() {
			if r.Vt.CommitVote == m.id {
				panic(fmt.Sprintf("node:%d instacnid:%d result:%d invalid msg:%+v", m.id, r.InstanceId, result, r))
			}
		} else if r.Vt.IsAccept() {
			if r.Vt.AcceptVote == m.id {
				panic(fmt.Sprintf("node:%d instacnid:%d result:%d  invalid msg:%+v", m.id, r.InstanceId, result, r))
			}
		}
		//fmt.Printf("ResultReport node:%d instacnid:%d result:%d value:%+v invalid msg:%+v\n", m.id, r.InstanceId, result, value, v.(*PaCommnMsg))
		//return -1

		//fmt.Printf("ResultReport need retry id:%d seqid:%d instanc:%d resultCode:%d  msg:%+v\n", m.id, r.Vt.Seq, iCurInstance, result, r)
		//如果失败了就要发起重试
		//这里需要异步的重试
	}

	return 0
}

func (m *PaNode) Step2(idx int) {
	for msg := range m.vecChans[idx] {
		//fmt.Printf("step2 id:%d idx:%d msg:%+v", m.id, idx, msg)
		//这里区分渠道进行处理，减少go的产生
		m.Step(&msg)
	}
}

func (m *PaNode) Step(t *PaCommnMsg) {
	//fmt.Printf("step except i:%d from:%d seq:%d flowtye:%d\n  ", m.id, t.Vt.FromId, t.GetSeqID(), t.Flowtype)
	//每次要更新最大的seq值
	for {
		tmpseq := atomic.LoadInt64(&m.curseq)
		if tmpseq < t.GetSeqID() {
			//更新最大的seq
			if atomic.CompareAndSwapInt64(&m.curseq, tmpseq, t.GetSeqID()) {
				break
			}
		} else {
			break
		}
	}

	r := m.GetSeqMsg(t.GetSeqID())

	//fmt.Printf("step nodid:%d r:%+v t:%+v\n", m.id, r, t)

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
		m.Commit(t, r)
	default:
		panic(fmt.Sprintf("nodeid:%d invalid flow type t:%+v m:%+v", m.id, t, r))
	}
}

//传指针
func (m *PaNode) Propose(t *PaCommnMsg, r *PaCommnMsg) {
	//提议阶段InForm
	FromId := t.Vt.FromId

	if t.IsAccept() {
		panic(fmt.Sprintf("invalid type curnodeid:%d m:%+v\n	t:%+v\n	[fromid:%d] %+v\n	accept vote msg:%+v", m.id, r, t, t.Vt.FromId, m.g.list[t.Vt.FromId].GetSeqMsg(t.Vt.Seq), m.g.list[t.Vt.AcceptVote].GetSeqMsg(t.Vt.Seq)))
	}

	r.Propose(t)

	t.SetFrom(m.id, PAXOS_MSG_PROPOSE_ACK)
	//这个肯定是要ack的
	go m.g.Sendto(FromId, *t)
}

func (m *PaNode) Commit(t *PaCommnMsg, r *PaCommnMsg) {
	if r.Commit(t) {
		m.ResultReport(r, PANODE_RESULT_COMMIT)
	}
}

func (m *PaNode) Accept(t *PaCommnMsg, r *PaCommnMsg) {
	if !t.IsAccept() {
		//自己都不accept，还要找我
		panic(fmt.Sprintf("id:%d tmsg:%+v", m.id, t))
	}
	FromId := t.Vt.FromId
	bSuc := r.Accept(t)
	if bSuc && r.Vt.AcceptVote != m.id {
		//fmt.Printf("[DEBUG]Accept invalid id:%d t:%+v r:%+v \n", m.id, t, r)
		//自己接受了，但是不是自己的值，这里其实已经失败了
		m.ResultReport(r, PANODE_RESULT_OTHER_ACCEPT)
	}

	t.SetFrom(m.id, PAXOS_MSG_ACCEPT_ACK)

	go m.g.Sendto(FromId, *t)
}

func (m *PaNode) ProposeAck(t *PaCommnMsg, r *PaCommnMsg) {
	if r.IsCommit() {
		//补充一个，如果自己预先已经commit了，将这个结果补充进去(千万不要以为自己想通了就删掉,之前删除过一次，遇到阻塞又加回来了)
		m.ResultReport(r, PANODE_RESULT_COMMIT)
		return
	}

	//t有可能是commit的
	if t.IsAccept() {
		if r.IsAccept() && !t.IsCommit() {
			//特殊的一种情况不需要处理
			return
		}
		/*
			if t.IsCommit() {
				fmt.Printf("[TRACE]proposeack node id:%d t commit:%+v r:%+v\n", m.id, t, r)
			}
		*/
		//异常已经accept的情况
		//fmt.Printf("iProposeAck has accepvote loc:%d seq:%d r:%+v t:%+v\n", m.id, t.Vt.Seq, r.Vt, t.Vt)
		//tmpProposeId := t.Vt.ProposeId
		//更新这个t的accept值
		//这时候是强制要接受这个值的，不需要比较proposeid
		//这里如果是0就尴尬了，后边引入0还是要放弃这个判断的
		/*
			if t.Vt.AcceptVote == m.id {
				//我自己都没有accept，远处传来一个accept，肯定异常的
				panic(fmt.Sprintf("seq:%d self:%d", t.Vt.Seq, m.id))
			}
		*/
		//还是不能强制accept

		//beforMsg := *r
		if !r.Accept(t) {
			//这里还是有可能因为proposeid而accept失败的，这种情况就需要忽略
			//fmt.Printf("[Debug] accept failed nodeid:%d t:%+v r:%+v\n", m.id, t, r)
			return
		}
		//fmt.Printf("[Debug] accept suc nodeid:%d t:%+v \nr:%+v \nbeforMsg:%+v\n", m.id, t, r, beforMsg)
		if !t.IsAccept() || !r.IsAccept() {
			panic(fmt.Sprintf("loc:%d t:%+v r:%+v", m.id, t, r))
		}
		//只能帮忙广播一次，最大的值进行广播，不一定会有广播，只是原封不动将这个t转发出去
		//不一定是提出者的
		/*
			if tmpProposeId <= r.Vt.ProposeId {
				t.SetFrom(t.Vt.AcceptVote, t.Flowtype)
				//fmt.Printf("Async go help accept loc:%d seq:%d t:%+v r:%+v\n", m.id, t.Vt.Seq, t, r)
				go m.g.Broadcastexcept(*t, m.id)
			}
		*/
		m.ResultReport(r, PANODE_RESULT_OTHER_ACCEPT)
		//fmt.Printf("ProposeAck failed need to get new propose :%+v\n", newres)
		//到这里自己的就需要主动放弃，寻求新的提议了
		//这里还不能决定到底有没有成功，只是当此的提交已经失败了
		return
	}

	//正常的提交流程
	bAccept, bretry := r.ProposeAck(t, m.g.GetNumber(), m.id)
	//如果进入到下一个阶段，需要发起broadcast
	if bAccept {
		//这里只能是接收自己
		//接上accept的逻辑，挡住后边的请求，自己先accept
		go m.g.Broadcastexcept(PaCommnMsg{
			Vt:       r.Vt,
			Flowtype: PAXOS_MSG_ACCEPT,
			Body:     r.Body,
		}, m.id)
	} else if bretry {
		//fmt.Printf("ProposeAck ProposeAck failed need to get new propose :%+v\n", newres)
		m.ResultReport(r, PANODE_RESULT_BIG_PROPOSEID)
	}
}

func (m *PaNode) AcceptAck(t *PaCommnMsg, r *PaCommnMsg) {
	//将数据写入本地的msg里
	bAccept, acceptVoted := r.AcceptAck(t, m.g.GetNumber()) // 返回是否落盘
	//判断是否能进入到下一个阶段
	if bAccept {
		if r.Vt.IsFailed() || t.Vt.IsFailed() {
			//这里数据已经不可能成功了
			//这里没有通知别人已经failed了，同样也是要通过commit告诉别人
			//我自己被别人拒绝了，但是别人应该也能感知到的
			m.ResultReport(r, PANODE_RESULT_IMPOSSIBLE)
			return
		}

		if !r.IsCommit() { //后边还是要去掉这个0的判断的，可以再最后的result来进行判断
			panic(fmt.Sprintf("acceptack error node:%d no commit t:%+v r:%+v", m.id, t, r))
		}

		//最终还是要通知这里的结果的，这里还是要通知所有人最后的决议
		m.g.InformVoteResult(VoteInfo{
			Seq:         r.Vt.Seq,
			ProposeId:   r.Vt.ProposeId,
			ProposeVote: r.Vt.ProposeVote,
			AcceptVote:  acceptVoted,
			CommitVote:  acceptVoted,
			FromId:      m.id,
		})

		//fmt.Printf("[TRACE][AcceptAck]commit done id:%d seq:%d acceptVoted:%d vt:%+v  r:%+v \n", m.id, t.Vt.Seq, acceptVoted, t, r)
		//最后再通知自己结果，这样的话，所有人就可以等到自己的结果了
		if acceptVoted != m.id {
			if r.Vt.IsFailed() {
				m.ResultReport(r, PANODE_RESULT_IMPOSSIBLE)
			} else {
				m.ResultReport(r, PANODE_RESULT_COMMIT)
			}
		} else {
			//只通知自己成功了
			m.ResultReport(r, PANODE_RESULT_SUC)
			//有可能别人帮他提交的，允许这种情况出现
			//减少通信的次数
			go m.g.Broadcastexcept(PaCommnMsg{
				Vt:       r.Vt,
				Flowtype: PAXOS_MSG_COMMIT,
				Body:     r.Body,
			}, m.id)
		}
	}
}

func (m *PaNode) GetSeqMsg(seq int64) *PaCommnMsg {
	v, exist := m.dict.Load(seq)
	if exist {
		return v.(*PaCommnMsg)
	}
	//创建一个新的msg
	var msgbody = PaCommnMsg{
		Vt: VoteInfo{
			Seq:    seq,
			FromId: m.id,
		},
	}
	//有可能捞到自己提交的值
	actMsgBody, _ := m.dict.LoadOrStore(seq, &msgbody)
	return actMsgBody.(*PaCommnMsg)
}

func (m *PaNode) GetId() int {
	return m.id
}

func (m *PaNode) Recv(t PaCommnMsg) {
	go m.Step(&t)
}

func (m *PaNode) Recv2(t PaCommnMsg) {
	m.vecChans[int(t.GetSeqID())%len(m.vecChans)] <- t
}
