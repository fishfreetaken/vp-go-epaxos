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

	dict sync.Map //[int64]*PaCommnMsg

	//这个只是用来进行保存本地提交的seq, 这个用instacneid to seqid key来触发处理
	mpLocPropose map[int64]*AckState
	lk           sync.Mutex
	reportmsg    chan int64

	veclk []sync.Mutex

	//每一个提交都建立一个唯一的事件id，这个id还是需要放到
	instanceid int64
	seqmap     map[int64]int64 //我要知道哪些是我自己的提出来的 ,key to instanceid

	//当前的
	curseq int64
	g      *PaGroup
}

func (m *PaNode) SetVecLkNums(g *PaGroup, membernum int) {
	for i := 0; i < 700; i++ {
		m.veclk = append(m.veclk, sync.Mutex{})
	}

	//在本地的函数中创建，在外边创建找不到
	m.mpLocPropose = make(map[int64]*AckState)
	m.seqmap = make(map[int64]int64)
	m.g = g
	m.reportmsg = make(chan int64, membernum*2)
}

func (m *PaNode) NewProPoseMsg(req *ClientReq, instacnid int64) *PaCommnMsg {
	//获取当前进度的seqid
	var iCurSeq int64
	var proposeid int32
	iCurInstanceId := atomic.AddInt64(&m.instanceid, 1)

	if instacnid != 0 {
		//用已有的进行注册
		iCurInstanceId = instacnid
	}
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
			Body:       req,
			InstanceId: iCurInstanceId,
		}
		//cm.ProposeList = append(cm.ProposeList, cm.Vt)
		_, load := m.dict.LoadOrStore(iCurSeq, &cm)
		if !load {
			//fmt.Printf("nodeid:%d begin goto seq:%d to proposeid:%d cm:%+v cm:%p\n", m.id, iCurSeq, proposeid, cm, &cm)
			//这里中间可能会发生改变，这个seq值被其他人占用
			if atomic.LoadInt32(&(cm.Vt.ProposeId)) <= proposeid {
				//这个值已经被别的抢占小了，重新发起
				break
			}

		}
		//这个值已经存在了，重新再找一个点
		//fmt.Printf("nodeid:%d has load this seq:%d newCurSeq:%d value:%+v\n", m.id, iCurSeq, atomic.LoadInt64(&m.curseq), value)
	}
	res := &PaCommnMsg{
		Vt: VoteInfo{
			ProposeId:   proposeid,
			ProposeVote: m.id,
			Seq:         iCurSeq,
			FromId:      m.id,
		},
		InstanceId: iCurInstanceId,
		Body:       req,
	}
	//fmt.Printf("[debug]NewProPoseMsg succnode:%d seq:%d instid:%d\n", m.id, iCurSeq, iCurInstanceId)
	//本地先注册
	//fmt.Printf("nodeid:%d instanceid:%d seqid:%d %d\n", m.id, iCurInstanceId, iCurSeq, m.curseq)
	m.RegisterSeq(res)
	//不需要等待
	m.g.Broadcast(res)
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
	t     time.Time
	seq   int64
	iCode int //返回的结果 0
}

func (m *PaNode) BuildProposeId() int32 {
	//这里是保证唯一的，而且如果有竞争的话，相对来说值越大的概率更高
	return int32(rand.Intn(100000)*GetPosCount(m.g.GetNumber()) + m.id)
}

func (m *PaNode) RegisterSeq(msg *PaCommnMsg) {
	m.lk.Lock()
	defer m.lk.Unlock()
	m.seqmap[msg.GetSeqID()] = msg.InstanceId
	if _, ok := m.mpLocPropose[msg.InstanceId]; ok {
		//这个在重试之前应该不太可能
		panic(fmt.Sprintf("has instanid:%d msg:%+v", m.id, msg))
	}
	//不管原来的是否存在
	m.mpLocPropose[msg.InstanceId] = &AckState{
		t:   time.Now(),
		seq: msg.GetSeqID(),
	}
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
	if len(m.reportmsg) == 0 {
		//自己没有要处理的消息就不用等了
		return
	}
	wg.Add(1)
	go m.AsyncReportAndRetry(wg)
}

func (m *PaNode) AsyncReportAndRetry(wg *sync.WaitGroup) {
	//设置一个15秒的超时时间
	t := time.NewTicker(time.Second * 15)

	defer func() {
		fmt.Printf("node id:%d has done\n", m.id)
		wg.Done()
		t.Stop()
	}()
	//本地如果没有提交的值可以不用等待
	for {
		select {
		case v := <-m.reportmsg:
			//都1️以最终的衡量
			bAllPass := true
			func() {
				m.lk.Lock()
				defer m.lk.Unlock()
				for _, st := range m.mpLocPropose {
					/*
						if v.iCode != PANODE_RESULT_SUC {
							bAllPass = false
						}
					*/
					if st.iCode == PANODE_RESULT_NOP {
						bAllPass = false
					}
				}
			}()
			if bAllPass {
				fmt.Printf("AsyncReportAndRetry suc new all pass id:%d retry insid:%+v\n", m.id, v)
				return
			}
		case <-t.C:
			var noDecideList []int64
			func() {
				m.lk.Lock()
				defer m.lk.Unlock()
				for key, v := range m.mpLocPropose {
					/*
						if v.iCode != PANODE_RESULT_SUC {
							noDecideList = append(noDecideList, key)
						}
					*/
					if v.iCode == 0 {
						noDecideList = append(noDecideList, key)
						fmt.Printf("no decide msg detail nodeid:%d instanceid :%d seq:%d msgdetail:%+v\n", m.id, key, v.seq, m.GetSeqMsg(v.seq))
					}
				}
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
	for instanceid, vt := range m.mpLocPropose {
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
		} else if vt.iCode == PANODE_RESULT_OTHER_ACCEPT {
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
	}
	return fmt.Sprintf("CalcLastReport [nodeid:%d][total:%d][suc:%d][bigpropose:%d][other_accept:%d][impossible:%d][other_case:%d]", m.id, len(m.mpLocPropose), cntsuc, cnt2, cnt3, cnt4, cntimpossible)
}

//异步发起进行重试
func (m *PaNode) ResultReport(r *PaCommnMsg, result int) (res int32) {
	m.lk.Lock()
	var iCurInstance int64
	var ok bool
	defer func() {
		m.lk.Unlock()
		if res == 0 {
			//只有正常了才能回传信息
			m.reportmsg <- iCurInstance
		}
	}()
	iCurInstance, ok = m.seqmap[r.Vt.Seq]
	if !ok {
		//本地没有注册就不需要了
		//会有一个乱序的问题，没有不代表以后没有
		res = -1
		return
		//panic(fmt.Sprintf("ResultReport node:%d result:%d msg:%+v ", m.id, result, r))
	}
	//索引对应的seq
	value, ok := m.mpLocPropose[iCurInstance]
	if !ok {
		//不可能会出现这种情况吧
		panic(fmt.Sprintf("[warning]ResultReport not exist node:%d ins:%d result:%d msg:%+v ", m.id, iCurInstance, result, r))
	}
	if value.seq != r.Vt.Seq || value.iCode != 0 {
		//fmt.Printf("[warning]ResultReport not equal nodeid:%d instance id :%d valueseq:%d r seq:%d  resultcode:%d res:%d\n", m.id, iCurInstance, value.seq, r.Vt.Seq, value.iCode, result)
		//insance所对应的seq已经发生变化，不需要进行处理
		res = -3
		return
	}
	value.iCode = result

	//check
	v, ok := m.dict.Load(value.seq)
	if !ok {
		panic(fmt.Sprintf("node:%d instacnid:%d invalid msg:%+v", m.id, iCurInstance, v.(*PaCommnMsg)))
	}

	if v.(*PaCommnMsg).InstanceId != iCurInstance {
		panic(fmt.Sprintf("r:%+v icur:%d", v, iCurInstance))
	}

	if result == PANODE_RESULT_SUC {
		if v.(*PaCommnMsg).Vt.AcceptVote != m.id {
			panic(fmt.Sprintf("node:%d instacnid:%d invalid msg:%+v", m.id, iCurInstance, v.(*PaCommnMsg)))
		}
	} else {
		if result != PANODE_RESULT_COMMIT {
			if v.(*PaCommnMsg).Vt.IsCommit() {
				if v.(*PaCommnMsg).Vt.CommitVote == m.id {
					panic(fmt.Sprintf("node:%d instacnid:%d result:%d value:%+v invalid msg:%+v", m.id, iCurInstance, result, value, v.(*PaCommnMsg)))
				}
			} else if v.(*PaCommnMsg).Vt.IsAccept() {
				if v.(*PaCommnMsg).Vt.AcceptVote == m.id {
					panic(fmt.Sprintf("node:%d instacnid:%d result:%d value:%+v invalid msg:%+v", m.id, iCurInstance, result, value, v.(*PaCommnMsg)))
				}
			}
			//fmt.Printf("ResultReport node:%d instacnid:%d result:%d value:%+v invalid msg:%+v\n", m.id, r.InstanceId, result, value, v.(*PaCommnMsg))
			//return -1
		}
		//fmt.Printf("ResultReport need retry id:%d seqid:%d instanc:%d resultCode:%d  msg:%+v\n", m.id, r.Vt.Seq, iCurInstance, result, r)
		//如果失败了就要发起重试
		//这里需要异步的重试
	}
	return 0
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

	m.veclk[int(t.GetSeqID())%len(m.veclk)].Lock()
	defer m.veclk[int(t.GetSeqID())%len(m.veclk)].Unlock()

	r := m.GetSeqMsg(t.GetSeqID())

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
		m.Commit(t, r)
	default:
		fmt.Printf("nodeid:%d invalid flow type t:%+v m:%+v", m.id, t, r)
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
	bSuc := r.Accept(t, false)
	if bSuc && r.Vt.AcceptVote != m.id {
		//fmt.Printf("[DEBUG]Accept invalid id:%d t:%+v r:%+v\n", m.id, t, r)
		//自己接受了，但是不是自己的值，这里其实已经失败了
		m.ResultReport(r, PANODE_RESULT_OTHER_ACCEPT)
	}

	t.SetFrom(m.id, PAXOS_MSG_ACCEPT_ACK)

	go m.g.Sendto(FromId, *t)
}

func (m *PaNode) ProposeAck(t *PaCommnMsg, r *PaCommnMsg) {
	if r.IsCommit() {
		//补充一个，如果自己预先已经commit了，将这个结果补充进去(千万不要以为自己想通了就删掉)
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
		tmpProposeId := t.Vt.ProposeId
		//更新这个t的accept值
		//这时候是强制要接受这个值的，不需要比较proposeid
		//t.Vt.UpdateProposeid(&r.Vt)

		if t.Vt.AcceptVote == m.id {
			//我自己都没有accept，远处传来一个accept，肯定异常的
			panic(fmt.Sprintf("seq:%d self:%d", t.Vt.Seq, m.id))
		}
		//这个是要强制accept的了，不需要判断id了
		if !r.Accept(t, true) {
			panic(fmt.Sprintf("accept failed nodeid:%d t:%+v r:%+v", m.id, t, r))
		}
		//if r.Vt.ProposeId > tProposeId {
		if !t.IsAccept() || !r.IsAccept() {
			panic(fmt.Sprintf("loc:%d t:%+v r:%+v", m.id, t, r))
		}
		//只能帮忙广播一次，最大的值进行广播，不一定会有广播，只是原封不动将这个t转发出去
		//不一定是提出者的
		if tmpProposeId <= r.Vt.ProposeId {
			t.SetFrom(t.Vt.AcceptVote, t.Flowtype)
			//fmt.Printf("Async go help accept loc:%d seq:%d t:%+v r:%+v\n", m.id, t.Vt.Seq, t, r)
			go m.g.Broadcastexcept(*t, m.id)
		}
		m.ResultReport(r, PANODE_RESULT_OTHER_ACCEPT)
		//fmt.Printf("ProposeAck failed need to get new propose :%+v\n", newres)
		//到这里自己的就需要主动放弃，寻求新的提议了
		//}
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
			Vt:         r.Vt,
			Flowtype:   PAXOS_MSG_ACCEPT,
			InstanceId: r.InstanceId,
			Body:       r.Body,
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

		go m.g.Broadcastexcept(PaCommnMsg{
			Vt:         r.Vt,
			Flowtype:   PAXOS_MSG_COMMIT,
			InstanceId: r.InstanceId,
			Body:       r.Body,
		}, m.id)

		fmt.Printf("[TRACE][AcceptAck]commit done id:%d seq:%d acceptVoted:%d vt:%+v  r:%+v \n", m.id, t.Vt.Seq, acceptVoted, t, r)
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
	actMsgBody, _ := m.dict.LoadOrStore(seq, &msgbody)
	return actMsgBody.(*PaCommnMsg)
}

func (m *PaNode) GetId() int {
	return m.id
}

func (m *PaNode) Recv(t PaCommnMsg) {
	go m.Step(&t)
}
