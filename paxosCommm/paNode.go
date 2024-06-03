package paxoscommm

import (
	"fmt"
	"math/rand"
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

	//这个只是用来进行保存本地提交的seq, 这个用instacneid来触发处理
	mpLocPropose map[int64]*AckState
	lk           sync.Mutex

	veclk []sync.Mutex

	//每一个提交都建立一个唯一的事件id，这个id还是需要放到
	instanceid int64
	seqmap     map[int64]int64 //我要知道哪些是我自己的提出来的

	//当前的
	curseq int64
	g      *PaGroup
}

func (m *PaNode) SetVecLkNums() {
	for i := 0; i < 300; i++ {
		m.veclk = append(m.veclk, sync.Mutex{})
	}
}

func (m *PaNode) NewProPoseMsg(req *ClientReq) *PaCommnMsg {
	//获取当前进度的seqid
	var iCurSeq int64
	var proposeid int32
	iCurInstanceId := atomic.AddInt64(&m.instanceid, 1)
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
	//本地先注册
	//fmt.Printf("nodeid:%d instanceid:%d seqid:%d\n", m.id, iCurInstanceId, iCurSeq)
	m.RegisterSeq(res)
	//广播给其他人
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

func (m *PaNode) NewCommitMsg(t *PaCommnMsg) PaCommnMsg {
	t.Vt.AcceptVote = m.id
	t.Vt.FromId = m.id
	t.Flowtype = PAXOS_MSG_COMMIT
	t.Vt.State = PAXOS_MSG_HAS_COMMITED
	return *t
}

type AckState struct {
	t     time.Time
	seq   int64
	iCode int //返回的结果 0
}

func (m *PaNode) BuildProposeId() int32 {
	//这里是保证唯一的，而且如果有竞争的话，相对来说值越大的概率更高
	return int32(rand.Intn(100000)*GetPosCount(m.g.GetNumber()) + m.id)
	//随机+priorityid
	offset := 10000
	newLocInc := atomic.AddInt32(&glocInc, 1)
	return int32(m.priority*offset + int(newLocInc)) //rand.Intn(offset)
}

func (m *PaNode) RegisterSeq(msg *PaCommnMsg) {
	m.lk.Lock()
	defer m.lk.Unlock()
	m.seqmap[msg.GetSeqID()] = msg.InstanceId
	if v, ok := m.mpLocPropose[msg.InstanceId]; ok {
		panic(fmt.Sprintf("has instanid:%d v:%+v msg:%+v", v, msg))
	}
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
)

func (m *PaNode) ResultReport(r *PaCommnMsg, result int) int32 {
	m.lk.Lock()
	defer m.lk.Unlock()
	iCurInstance, ok := m.seqmap[r.Vt.Seq]
	if !ok {
		return 0
	}
	if iCurInstance != r.InstanceId {
		panic(fmt.Sprintf("r:%+v icur:%d", r, iCurInstance))
	}
	value, ok := m.mpLocPropose[r.InstanceId]
	if !ok {
		//不存在
		return 0
	}
	if value.iCode != 0 {
		return 0
	}
	value.iCode = result
	v, ok := m.dict.Load(value.seq)
	if !ok {
		panic(fmt.Sprintf("node:%d instacnid:%d invalid msg:%+v", m.id, r.InstanceId, v.(*PaCommnMsg)))
	}

	if result != PANODE_RESULT_SUC {
		if v.(*PaCommnMsg).Vt.AcceptVote == m.id {
			fmt.Printf("ResultReport node:%d instacnid:%d result:%d value:%+v invalid msg:%+v\n", m.id, r.InstanceId, result, value, v.(*PaCommnMsg))
			return -1
			//panic(fmt.Sprintf("node:%d instacnid:%d result:%d value:%+v invalid msg:%+v", m.id, instanceid, result, value, v.(*PaCommnMsg)))
		}
	} else {
		if v.(*PaCommnMsg).Vt.AcceptVote != m.id {
			panic(fmt.Sprintf("node:%d instacnid:%d invalid msg:%+v", m.id, r.InstanceId, v.(*PaCommnMsg)))
		}
	}
	return 0
}

func (m *PaNode) CalcLastSuccRate(cnt, sumcnt *int) string {
	var suncnt int
	var code1 int
	var code2 int
	var code3 int
	var code4 int
	for seqid, vt := range m.mpLocPropose {
		v, ok := m.dict.Load(seqid)
		if !ok {
			continue
		}
		t := v.(*PaCommnMsg)
		if t.Vt.IsAccept() && m.g.mpResult[t.Vt.Seq] == m.id {
			if vt.iCode != PANODE_RESULT_SUC {
				//panic(fmt.Sprintf("nodid:%d seq:%d code:%d", m.id, t.Vt.Seq, vt.iCode))
				code4++
			}
			suncnt++
		}
		if vt.iCode == PANODE_RESULT_BIG_PROPOSEID {
			code1++
		} else if vt.iCode == PANODE_RESULT_OTHER_ACCEPT {
			code2++
		} else if vt.iCode == PANODE_RESULT_IMPOSSIBLE {
			code3++
		}
	}
	*cnt += suncnt
	*sumcnt += len(m.mpLocPropose)

	return fmt.Sprintf("CalcLastSuccRate[nodeid:%d][succnt:%d:%d][totalcnt:%d][sucrate:%f][Bigpropose:%f][otheraccept:%f][impossilbe:%f]", m.id, suncnt, code4, len(m.mpLocPropose),
		GetPercent(suncnt, len(m.mpLocPropose)), GetPercent(code1, len(m.mpLocPropose)), GetPercent(code2, len(m.mpLocPropose)), GetPercent(code3, len(m.mpLocPropose)))
}

//允许自定义控制proposeid
func (m *PaNode) BeginCommit(req *ClientReq, wg *sync.WaitGroup) int64 {
	//有自己构造proposeid的能力
	wg.Add(1)
	//根据seqid 构建消息
	msg := m.NewProPoseMsg(req)
	//msg.Body = req
	//还是原来的值，这里异步已经接受了其他人的值，这里再发起的时候就已经变了值的，所以这里不能用原来的指针，需要重新创建一个新的值，与原来的进行剥离
	//wg.Done()
	//给一个事件的注册中心来注册，等待响应结果，要么等待超时
	wg.Done()
	//早早的退出，统一收归进行消息处理,快速进行下一次的提问
	//设置超时时间

	return msg.Vt.Seq
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

func (m *PaNode) Accept(t *PaCommnMsg, r *PaCommnMsg) {
	if !t.IsAccept() {
		//自己都不accept，还要找我
		panic(fmt.Sprintf("id:%d tmsg:%+v", m.id, t))
	}
	FromId := t.Vt.FromId
	bSuc := r.Accept(t)

	if bSuc && r.IsAccept() && r.Vt.AcceptVote != m.id {
		//fmt.Printf("invalid id:%d t:%+v r:%+v", m.id, t, r)
		//自己接受了，但是不是自己的值，这里其实已经失败了
		tres := m.ResultReport(r, PANODE_RESULT_OTHER_ACCEPT)
		if tres != 0 {
			panic(fmt.Sprintf("invalid id:%d t:%+v r:%+v", m.id, t, r))
		}
	}

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
		//fmt.Printf("iProposeAck has accepvote loc:%d seq:%d r:%+v t:%+v\n", m.id, t.Vt.Seq, r.Vt, t.Vt)
		tmpProposeId := t.Vt.ProposeId
		//更新这个t的accept值
		//这时候是强制要接受这个值的，不需要比较proposeid
		t.Vt.UpdateProposeid(&r.Vt)

		if t.Vt.AcceptVote == m.id {
			//我自己都没有accept，远处传来一个accept，肯定异常的
			panic(fmt.Sprintf("seq:%d self:%d", t.Vt.Seq, m.id))
		}

		if !r.Accept(t) {
			panic("accept failed")
		}
		//if r.Vt.ProposeId > tProposeId {
		if !t.IsAccept() || !r.IsAccept() {
			panic(fmt.Sprintf("loc:%d t:%+v r:%+v", m.id, t, r))
		}
		//只能帮忙广播一次，最大的值进行广播，不一定会有广播，只是原封不动将这个t转发出去
		//不一定是提出者的
		if tmpProposeId <= r.Vt.ProposeId {
			t.SetFrom(t.Vt.AcceptVote, PAXOS_MSG_ACCEPT)
			//fmt.Printf("Async go help accept loc:%d seq:%d t:%+v r:%+v\n", m.id, t.Vt.Seq, t, r)
			go m.g.Broadcastexcept(*t, m.id)
		}

		m.ResultReport(r, PANODE_RESULT_OTHER_ACCEPT)
		//newres := m.NewProPoseMsg(r.Body.(*ClientReq))
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
		//newres := m.NewProPoseMsg(r.Body.(*ClientReq))
		//fmt.Printf("ProposeAck ProposeAck failed need to get new propose :%+v\n", newres)
		m.ResultReport(r, PANODE_RESULT_BIG_PROPOSEID)
	}
}

func (m *PaNode) AcceptAck(t *PaCommnMsg, r *PaCommnMsg) {
	//将数据写入本地的msg里
	bAccept, acceptVoted := r.AcceptAck(t, m.g.GetNumber()) // 返回是否落盘
	//判断是否能进入到下一个阶段
	if bAccept {
		if acceptVoted != m.id {
			//fmt.Printf("commit done id:%d seq:%d acceptVoted:%d vt:%+v  r:%+v \n", m.id, t.Vt.Seq, acceptVoted, t.Vt, r.Vt)
		} else {
			//只通知自己成功了
			m.ResultReport(r, PANODE_RESULT_SUC)
		}

		//这里有并发的问题，并不知道是否已经close，超时也是有可能会在if判断之后，close的
		//todo commit 提交所有人的决议
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
