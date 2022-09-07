package halo

import (
	"golang.org/x/exp/slices"
)

const (
	RoundRobin = iota
	WeightRobin
)

type election interface {
	add(client *serverInfo)
	next() string
	remove(client *serverInfo)
	setWegiht(num int, addr string)
}

type roundRobinBalance struct {
	curIndex int
	addrList []string
}

func (b *roundRobinBalance) add(client *serverInfo) {
	if !slices.Contains(b.addrList, client.addr) {
		b.addrList = append(b.addrList, client.addr)
	}
}
func (b *roundRobinBalance) setWegiht(num int, addr string) {}

func (b *roundRobinBalance) remove(client *serverInfo) {
	index := -1
	for key, value := range b.addrList {
		if value == client.addr {
			index = key
		}
	}
	if index > -1 {
		if (index + 1) == len(b.addrList) {
			b.addrList = b.addrList[:index]
		} else {
			b.addrList = append(b.addrList[:index], b.addrList[:index+1]...)
		}
	}
}

func (b *roundRobinBalance) next() string {
	len := len(b.addrList)
	if len == 0 {
		return ""
	}
	if b.curIndex >= len {
		b.curIndex = 0
	}
	addr := b.addrList[b.curIndex]
	b.curIndex = (b.curIndex + 1) % len
	return addr
}

type weightRoundRobinBalance struct {
	curAddr  string
	addrList map[string]*node
}

type node struct {
	weght         int
	currentWeight int
	stepWeight    int
	addr          string
}

func (b *weightRoundRobinBalance) add(client *serverInfo) {
	if _, ok := b.addrList[client.addr]; !ok {
		node := &node{
			weght:         client.weight,
			currentWeight: client.weight,
			stepWeight:    client.weight,
			addr:          client.addr,
		}
		b.addrList[client.addr] = node
	}
}

func (b *weightRoundRobinBalance) next() string {
	if len(b.addrList) == 0 {
		return ""
	}
	totalWight := 0
	var maxWeghtNode *node
	for key, value := range b.addrList {
		totalWight += value.stepWeight
		value.currentWeight += value.stepWeight
		if maxWeghtNode == nil || maxWeghtNode.currentWeight < value.currentWeight {
			maxWeghtNode = value
			b.curAddr = key
		}
	}
	maxWeghtNode.currentWeight -= totalWight
	return maxWeghtNode.addr
}

func (b *weightRoundRobinBalance) remove(client *serverInfo) {
	delete(b.addrList, client.addr)
}

func (b *weightRoundRobinBalance) setWegiht(num int, addr string) {
	if node, ok := b.addrList[addr]; ok {
		if num > 0 && node.weght > node.stepWeight {
			if (node.stepWeight + num) > node.weght {
				node.stepWeight = node.weght
			} else {
				node.stepWeight += num
			}
		}
		if num == -1 {
			node.stepWeight = -1
		}
	}
}
