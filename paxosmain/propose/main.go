package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"paxosmain/paxoscomm"
)

func main() {
	seqnum := 10000
	var g = paxoscomm.Group{
		Totalseq: seqnum,
	}
	g.Init(10)
	go func() {
		http.ListenAndServe("0.0.0.0:80", nil)
	}()

	fmt.Printf("go begin")

	for i := 0; i < seqnum; i++ {
		go func(seq int) {
			go g.Index(1).ToBeProposemaster(seq, -1)
			go g.Index(4).ToBeProposemaster(seq, -1)
			go g.Index(7).ToBeProposemaster(seq, -1)
			go g.Index(2).ToBeProposemaster(seq, -1)
		}(i)
		g.RefreshSeq(i + 1)
	}

	g.Wait(-1)
	//time.Sleep(time.Second * 3)
	for i := 0; i < seqnum; i++ {
		g.Report(i)
	}
	g.GetLastCalc()
}
