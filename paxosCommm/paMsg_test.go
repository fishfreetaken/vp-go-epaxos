package paxoscommm

import (
	"fmt"
	"testing"
)

func TestSetVoteInfoAccept(t *testing.T) {
	var sv = VoteInfo{
		ProposeId:   330008,
		Seq:         5,
		ProposeVote: 8,
		State:       1,
		CommitVote:  0,
		AcceptVote:  8,
	}
	var r = VoteInfo{
		ProposeId:   5442505,
		Seq:         5,
		ProposeVote: 5,
		AcceptVote:  0,
		State:       0,
	}
	fmt.Println(r.SetAccept(&sv), r.IsAccept())

	fmt.Printf("sv:%+v r:%+v\n", sv, r)

}

//
func TestSetVoteInfoAckAccept(t *testing.T) {
	var sv = PaCommnMsg{
		Vt: VoteInfo{
			ProposeId:   5442505,
			Seq:         5,
			ProposeVote: 5,
			AcceptVote:  5,
			State:       1,
		},
	}

	var r = PaCommnMsg{
		Vt: VoteInfo{
			ProposeId:   330008,
			Seq:         5,
			ProposeVote: 8,
			AcceptVote:  8,
			State:       1,
			CommitVote:  -2,
		},
	}

	r.AcceptAck(&sv, 10)
	fmt.Printf("sv:%+v r:%+v\n", sv, r)
}

func TestSetAcceptAck(t *testing.T) {
	var m = PaCommnMsg{
		Vt: VoteInfo{
			ProposeId:   1,
			ProposeVote: 9,
			AcceptVote:  9,
			State:       1,
			FromId:      9,
		},
	}
	ttcount := 11
	for i := 0; i < 5; i++ {
		m.AcceptAck(&PaCommnMsg{
			Vt: VoteInfo{
				ProposeId:   1,
				ProposeVote: 9,
				AcceptVote:  9,
				State:       1,
				FromId:      i,
			},
		}, ttcount)
	}
	for i := 5; i < 9; i++ {
		m.AcceptAck(&PaCommnMsg{
			Vt: VoteInfo{
				ProposeId:   1,
				ProposeVote: 9,
				AcceptVote:  9,
				State:       1,
				FromId:      i,
			},
		}, ttcount)
	}
}
