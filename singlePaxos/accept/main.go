package main

import (
	"paxosmain/paxoscomm"
)

// 不可修改性，一旦accept，就不能更改值
// 已经accept的值不能再accept其他值
// 获得半数以上accept的，成为主，执行完命令，将最后的状态commit到其他节点上
// commit 到其他节点上形成一致
// 主要问题：选票可能被瓜分，成功率不高，可以看下qps , 如果失败，只能决断下一个seq了

// 解决办法，各个提交的时间尽量随机，但影响qps

// version 1.0 一旦accept就不能更改,选票被瓜分
// version 2.0 如果一个人选票过半数，他就是master
// version 3.0 我要先知道集群有没有accept，如果没有，最大的proposid是多少

func main() {
	seqnum := 6000
	var g paxoscomm.Group
	g.Totalseq = seqnum
	g.Init(10)

	for i := 0; i < seqnum; i++ {
		go func(seq int) {
			go g.Index(1).ToBemaster(seq)
			go g.Index(4).ToBemaster(seq)
			go g.Index(7).ToBemaster(seq)
			go g.Index(2).ToBemaster(seq)
			//go g.Index(8).ToBemaster(seq)
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
