package main

import (
	"fmt"
	"math/bits"
	"math/rand"
	"time"
)

const (
	SKIPLIST_MAXLEVEL int = 32
)

type skipListNode struct {
	score    float64           // 分值
	element  *GObj             // 值
	backward *skipListNode     // 后向链指针
	level    []*zskiplistLevel // 链层节点
}

type zskiplistLevel struct {
	forward *skipListNode // 链向的下一个节点
	span    int           // span指距离前一个同层节点几个节点距离
}

type skipList struct {
	SkipListType
	head *skipListNode
	tail *skipListNode

	r      *rand.Rand
	length int
	level  int // 这里记录的是最高level
}

type SkipListType struct {
	CompareFunc func(k1, k2 *GObj) int // 比对key是否相等
}

type ZSet struct {
	zsl  *skipList
	dict *Dict
}

func ZSetCreate(skipListType SkipListType) *ZSet {
	z := &ZSet{
		dict: DictCreate(DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
		zsl: &skipList{
			level:        1,
			SkipListType: skipListType,
		},
	}
	z.zsl.resetRand()
	return z
}

func (z *skipList) resetRand() {
	z.r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func (z *skipList) randLevel() int {
	// 2^n-1
	total := uint64(1)<<uint64(SKIPLIST_MAXLEVEL) - 1
	// 求出层高
	// 512<k<1023之间的概率为1/2，level=1
	// 256<k<511之间的概率为1/4，level=2
	// 128<k<255之间的概率为1/8，level=3
	// k会落在某个范围
	k := z.r.Uint64() & total
	// bits.Len64(k) 返回k的二进制的最高位的位数，例如5 -> 101 -> 3
	return SKIPLIST_MAXLEVEL - bits.Len64(k) + 1
}

func newSkipListNode(level int, score float64, elem *GObj) *skipListNode {
	return &skipListNode{
		score:   score,
		element: elem,
		level:   make([]*zskiplistLevel, level),
	}
}

func (z *skipList) insertInner(score float64, elem *GObj) {
	var (
		update [SKIPLIST_MAXLEVEL]*skipListNode
		rank   [SKIPLIST_MAXLEVEL]int
	)
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		if i == z.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		// 要么前面节点比我分数小，如果相同的话前面节点的值不能相等
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && z.CompareFunc(x.level[i].forward.element, elem) < 0)) {
			// 更新跨越的span
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}
	// 新插入节点
	newLevel := z.randLevel()
	// 新level比现存的还大
	if newLevel > z.level {
		// 补齐顶头
		for i := z.level; i < newLevel; i++ {
			rank[i] = 0
			// head是最高的塔，交给他补齐
			update[i] = z.head
			// ???
			update[i].level[i].span = z.length
		}
		z.level = newLevel
	}
	// 构建新塔
	x = newSkipListNode(newLevel, score, elem)
	for i := 0; i < newLevel; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x
		// TODO 这里有点难懂
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := newLevel; i < z.level; i++ {
		update[i].level[i].span++
	}

	if update[0] != z.head {
		x.backward = update[0]
	}

	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		z.tail = x
	}

	z.length++
}


func (z *skipList) Get(score float64) *GObj {
	x := z.head
	for i := z.level; i >= 0; i-- {
		// 向右走
		for x.level[i].forward != nil && x.level[i].forward.score < score {
			x = x.level[i].forward
		}
	}
	x = x.level[0].forward
	if x != nil && score == x.score {
		return x.element
	}
	return nil
}

func (z *skipList) deleteNode(x *skipListNode, update []*skipListNode) {
	for i := 0; i < z.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span -= 1
		}
	}

	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		z.tail = x.backward
	}
	// 删除节点后降低层高
	for z.level > 1 && z.head.level[z.level-1].forward == nil {
		z.level--
	}
	z.length--
}

func (z *skipList) delete(score float64, elem *GObj) *skipListNode {
	var update [SKIPLIST_MAXLEVEL]*skipListNode
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				x.level[i].forward.score == score &&
					z.CompareFunc(x.level[i].forward.element, elem) < 0) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	if x != nil && score == x.score && z.CompareFunc(x.element, elem) == 0 {
		z.deleteNode(x, update[:])
		return x
	}
	return x
}

func (z *ZSet) Add(score float64, mem *GObj) {
	// 找有无对应的key
	e := z.dict.Find(mem)
	if e != nil {
		// 存在这个entry，只score
		curScoreString := e.Val.StrVal()
		if fmt.Sprintf("%f", score) != curScoreString {
			// 删掉旧的元素
			z.zsl.delete(score, mem)
			// 插入新的
			z.zsl.insertInner(score, mem)
			// TODO 奇怪，为什么要incr
			mem.IncrRefCount()
		}
	}

}
