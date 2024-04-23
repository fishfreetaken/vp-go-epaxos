package paxoscomm

type pmsg struct {
	seq     int //当前序号
	from    int
	msgtype int

	vote        int //我选的是谁，谁发起的提议 accept
	proposevote int //预提交的选择谁
	proposid    int

	acceptids   []pmsg //接收的有哪些
	preproseids []pmsg
}

func newpmsg(seq int) pmsg {
	return pmsg{
		seq:         seq,
		vote:        -1,
		proposevote: -1,
	}
}

func newproposepmsg(seq, proposeid, vote int) pmsg {
	return pmsg{
		seq:         seq,
		proposevote: vote,
		proposid:    proposeid,
		vote:        -1,
		from:        vote,
	}
}

func newacceptmsg(seq, vote int) pmsg {
	return pmsg{
		seq:     seq,
		vote:    vote,
		from:    vote,
		msgtype: MSG_TYPE_ACCEPT,
	}
}

func (r *pmsg) refreshpropose(kc *pmsg) bool {

	//保留原始状态插入，后更新
	bFind := false
	for idx, v := range r.preproseids {
		if v.from == kc.from {
			//fmt.Printf("getmasterpreprose error case 2 kc%+v r:%+v\n", kc, r)
			bFind = true
			if r.preproseids[idx].proposid < kc.proposid {
				r.preproseids[idx].proposid = kc.proposid
				r.preproseids[idx].proposevote = kc.proposevote
			}
		}
	}
	//找下一个
	if !bFind {
		r.preproseids = append(r.preproseids, *kc)
	}

	if r.proposid < kc.proposid {
		//我没有accept任何值，而且你的proproseid比我的大，那我就接受你
		r.proposid = kc.proposid
		r.proposevote = kc.proposevote
	}

	kc.vote = r.vote
	kc.proposid = r.proposid
	kc.proposevote = r.proposevote
	return true
}

func (r *pmsg) maxproposeexceep(nodenum int) bool {
	cnt := 0
	for _, v := range r.preproseids {
		if v.proposid == r.proposid {
			cnt++
		}
	}
	level := (nodenum>>1 + 1)
	return cnt >= level
}

func (r *pmsg) resetpropose(vote, proposeid int) {
	r.vote = -1
	r.proposevote = vote
	r.proposid = proposeid
	r.msgtype = MSG_TYPE_PROPOSE
	r.acceptids = []pmsg{}
	r.preproseids = []pmsg{}

}
func (r *pmsg) isaccept() bool {
	return r.vote != -1
}

func (r *pmsg) accept(kc *pmsg) bool {
	bAccept := false
	if !r.isaccept() {
		bAccept = true
		r.vote = kc.vote
	}
	kc.proposid = r.proposid
	kc.vote = r.vote
	return bAccept
}

func (r *pmsg) accepttarget(id int) bool {
	if r.isaccept() {
		return false
	}
	r.vote = id
	return true
}

func (r *pmsg) getmasterpreprose(nodenum int) (rvote, curMaxproprosid int) {

	level := (nodenum>>1 + 1)
	if len(r.preproseids) < level {
		return
	}
	var st = make(map[int]int)
	for _, v := range r.preproseids {
		if v.proposid == r.proposid {
			st[v.proposevote]++
		}
	}

	for vote, num := range st {
		if num >= level {
			rvote = vote
		}
		if vote > curMaxproprosid {
			curMaxproprosid = vote
		}
	}
	return
}

func (r *pmsg) ishasaccepted(kc *pmsg) bool {
	for _, v := range r.acceptids {
		if v.from == kc.from {
			return true
		}
	}
	//fmt.Printf("ishasaccepted seq:%d id:%d accept kc:%+v\n", r.seq, r.proposevote, kc)
	r.acceptids = append(r.acceptids, *kc)
	return false
}

func (r *pmsg) getmasteraccept(nodenum int) int {
	level := (nodenum>>1 + 1)
	if len(r.acceptids) < level {
		return -2
	}
	var cal = make(map[int]int)
	for _, v := range r.acceptids {
		cal[v.vote]++
		if cal[v.vote] >= level {
			return v.vote
		}
	}
	return -1
}
