package paxoscomm

import (
	"fmt"
	"math/rand"
)

// 肯定都是选自己的
type Pnode struct {
	id       int //节点id
	recv     chan pmsg
	accepted []pmsg // seq -> 已经成立的消息
	g        *Group
}

const (
	MSG_TYPE_PROPOSE = iota
	MSG_TYPE_PROPOSE_ACK
	MSG_TYPE_ACCEPT
	MSG_TYPE_ACCEPT_ACK
)

func (m *Pnode) receive() {
	for v := range m.recv {
		fromid := v.from
		var r *pmsg
		switch v.msgtype {
		case MSG_TYPE_PROPOSE: //回包
			r = m.preprose(&v)
		case MSG_TYPE_PROPOSE_ACK: //不用回包
			m.preproseack(&v)
		case MSG_TYPE_ACCEPT: //回包
			r = m.accept(&v)
		case MSG_TYPE_ACCEPT_ACK: //不用回包
			m.accceptack(&v)
		default:
			panic(fmt.Errorf("invalid type %d", v.msgtype))
		}
		if r != nil {
			r.from = m.id
			//fmt.Printf("msg send loc:%d from:%d v:%d\n", m.id, r.from, v.from)
			m.g.send(fromid, *r)
		}
	}
}

func (m *Pnode) getmsg(seq int) *pmsg {
	for i := len(m.accepted); i <= seq; i++ {
		m.accepted = append(m.accepted, newpmsg(i))
	}
	return &m.accepted[seq]
}

// 提出proposeid
func (m *Pnode) preprose(kc *pmsg) *pmsg {

	r := m.getmsg(kc.seq)
	r.refreshpropose(kc)
	kc.msgtype = MSG_TYPE_PROPOSE_ACK

	return kc
}

// 要么我提名成功，要么我commit成功
func (m *Pnode) preproseack(kc *pmsg) {
	// 如果当前有在accept，结束流程，我直接走accept的逻辑
	// 如果有最大的propose的话，我就退出，占下一个seq
	// 如果有跟我一样的propose，我就再随机一个数发起流程
	r := m.getmsg(kc.seq)
	if r.isaccept() {
		//nop 如果自己accept了，返回给clien决定,如果是accept自己的话就继续等，如果其他人换一个seq
		//fmt.Printf("error has accept id :%d %d\n", r.proposid, kc.proposid)
		return
	} else if kc.isaccept() {
		m.accept(kc)
		return
	}

	//需要一个协调的定时器，能发起重试 大概在100ms左右，毕竟来回30ms就行了
	r.refreshpropose(kc)

	masterVote, _ := r.getmasterpreprose(m.g.len())
	if masterVote == -1 { //没有选出来主，就不需要了
		//没有过半数 暂时不要管
		return
	}

	//如果发现提议更大的，更新随机等待下一个时间段，再开始propose
	//已经

	// 超时之前看自己是否过半数，如果过了，就发起
	// 1 过半数了，发现提议还是自己，进入accept
	if m.id == masterVote {
		r.accepttarget(m.id)
		//先提交自己，再同步其他人
		//fmt.Printf("suc gen accept getmasterpreprose id:%d kc:%+v r:%+v\n", m.id, kc, r)
		go m.ToBemasterExceptMe(kc.seq)
		return
	}
	/*
		//用定时器异步去支持
			else if !r.maxproposeexceep(m.g.len()) { //看当前的最大值是否过半数，过半数就不参与竞争了
				// 2 过半数，发现提议的是别人，用当前最大的提议值，去重新发起一个提议
				// 这个保证只能执行一次
				newProposeid := r.proposid + rand.Intn(20)
				fmt.Printf("getmasterpreprose retry propose seq:%d cur_id:%d newProposeid:%d r:%+v \n", kc.seq, m.id, newProposeid, r)
				r.resetpropose(m.id, newProposeid)
				//go m.ToBeProposemasterExceptme(kc.seq, r.proposid+rand.Intn(20))
				return
			}
	*/
	//默认就不参与了
	//fmt.Printf("getmasterpreprose do null id:%d r:%+v kc:%+v\n", m.id, r, kc)
}

// accept 后还是要返回数据的
func (m *Pnode) accept(kc *pmsg) *pmsg {
	r := m.getmsg(kc.seq)
	//fmt.Printf("accept loc:%d from %d fromvote:%d r vote:%d\n", m.id, kc.from, kc.vote, r.vote)
	if r.accept(kc) {
		//fmt.Printf("accept suc loc:%d from %d fromvote:%d r vote:%d\n", m.id, kc.from, kc.vote, r.vote)
	} else {
		//fmt.Printf("accept reject loc:%d from %d fromvote:%d r vote:%d\n", m.id, kc.from, kc.vote, r.vote)
	}
	kc.msgtype = MSG_TYPE_ACCEPT_ACK
	return kc
}

func (m *Pnode) acceptself(seq int) {
	r := m.getmsg(seq)
	if r.isaccept() {
		fmt.Printf("acceptself has accepedt r:%+v", r)
		return
	}
	r.vote = m.id
}

// 我要知道自己是否是老大
func (m *Pnode) accceptack(kc *pmsg) {
	r := m.getmsg(kc.seq)
	//fmt.Printf("accceptack loc:%d from:%d \n", m.id, kc.from)
	if r.ishasaccepted(kc) {
		//幂等处理,基本不可能发生的情况，除非下游重试发送消息
		panic(*kc)
	}

	//我的提议或者别人的提议是否超过半数，别人超过认别人当老大，自己超过就认自己老大
	masterVote := r.getmasteraccept(m.g.len())
	if masterVote >= 0 && masterVote == m.id {
		// i win
		m.g.calcVoteResult(kc.seq, masterVote)
		//fmt.Printf("i win masterVote:%d seq:%d id:%d get master from:%d \n", masterVote, r.seq, m.id, kc.from)
	} else if len(r.acceptids) == m.g.len() && masterVote == -1 {
		m.g.calcVoteResult(kc.seq, -1)
	}
	//fmt.Printf("id:%d list :%+v\n", m.id, r.acceptids)
}

func (n *Pnode) ToBemaster(seq int) {
	//除了自己，广播给所有人
	n.g.broadcast(newacceptmsg(seq, n.id))
}

func (n *Pnode) ToBemasterExceptMe(seq int) {
	n.g.broadcastexcept(newacceptmsg(seq, n.id), n.id)
}

func (n *Pnode) ToBeProposemaster(seq int, proposid int) {
	if proposid == -1 {
		proposid = rand.Intn(n.g.len() * 1000) //随机出来一个值，防止冲突
	}
	//fmt.Printf("seq:%d node:%d proposid:%d\n ", seq, n.id, proposid)
	//广播给所有人
	n.g.broadcast(newproposepmsg(seq, proposid, n.id))
}

func (n *Pnode) ToBeProposemasterExceptme(seq int, proposid int) {
	n.g.broadcastexcept(newproposepmsg(seq, proposid, n.id), n.id)
}
