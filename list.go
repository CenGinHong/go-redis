package main

type Node struct {
	Val  *GObj
	next *Node
	prev *Node
}

// 判断两个元素是否一样，这样才能移除元素
type ListType struct {
	EqualFunc func(a, b *GObj) bool
}

type List struct {
	ListType // 使用组合方式，这里能不能用接口？
	head     *Node
	// tail     *Node
	length int
}

func ListCreate(listType ListType) *List {
	// var list List
	// list.ListType = listType
	dummyHead := &Node{
		Val: CreateObject(GSTR, nil),
	}
	dummyHead.next = dummyHead
	dummyHead.prev = dummyHead
	return &List{
		ListType: listType,
		head:     dummyHead,
	}
}

func (list *List) Length() int {
	return list.length
}

func (list *List) First() *Node {
	// return list.head
	if list.length == 0 {
		return nil
	}
	return list.head.next
}

func (list *List) Last() *Node {
	// return list.tail
	if list.length == 0 {
		return nil
	}
	return list.head.prev
}

func (list *List) Find(val *GObj) *Node {
	// p := list.head
	// for p != nil {
	// 	if list.EqualFunc(p.Val, val) {
	// 		break
	// 	}
	// 	p = p.next
	// }
	// return p
	var ret *Node
	for p := list.head.next; p != list.head; p = p.next {
		if list.EqualFunc(p.Val, val) {
			ret = p
			break
		}
	}
	return ret
}

func (list *List) Append(val *GObj) {
	// n := &Node{
	// 	Val: val,
	// }
	// if list.head == nil {
	// 	list.head = n
	// 	list.tail = n
	// } else {
	// 	n.prev = list.tail
	// 	list.tail.next = n
	// 	list.tail = list.tail.next
	// }
	// list.length++
	n := &Node{
		Val: val,
	}
	p := list.head.prev
	p.next = n
	n.prev = p
	list.head.prev = n
	n.next = list.head
	list.length++
}

func (list *List) LPush(val *GObj) {
	// n := &Node{
	// 	Val: val,
	// }
	// if list.head == nil {
	// 	list.head = n
	// 	list.tail = n
	// } else {
	// 	n.next = list.head
	// 	list.head.prev = n
	// 	list.head = n
	// }
	// list.length++

	n := &Node{
		Val: val,
	}
	p := list.head.next
	n.next = p
	p.prev = n
	list.head.next = n
	n.prev = list.head
	list.length++
}

func (list *List) delNode(n *Node) {
	// if n == nil {
	// 	return
	// }
	// if list.head == n {
	// 	n.next.prev = nil
	// 	list.head = n.next
	// 	n.next = nil
	// } else if list.tail == n {
	// 	n.prev.next = nil
	// 	list.tail = n.prev
	// 	n.prev = nil
	// } else {
	// 	n.prev.next = n.next
	// 	n.next.prev = n.prev
	// 	n.prev = nil
	// 	n.next = nil
	// }
	// list.length--
	if n == nil || list.length == 0 {
		return
	}
	n.prev.next = n.next
	n.next.prev = n.prev
	n.next = nil
	n.prev = nil
	list.length--
}

func (list *List) Delete(val *GObj) {
	list.delNode(list.Find(val))
}
