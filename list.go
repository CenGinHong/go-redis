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
	ListType 
	head     *Node
	tail     *Node
	length int
}

func ListCreate(listType ListType) *List {
	var list List
	list.ListType = listType
	return &list
}

func (list *List) Length() int {
	return list.length
}

func (list *List) First() *Node {
	return list.head
}

func (list *List) Last() *Node {
	return list.tail
}

func (list *List) Find(val *GObj) *Node {
	p := list.head
	for p != nil {
		if list.EqualFunc(p.Val, val) {
			break
		}
		p = p.next
	}
	return p
}

func (list *List) Append(val *GObj) {
	n := &Node{
		Val: val,
	}
	if list.head == nil {
		list.head = n
		list.tail = n
	} else {
		n.prev = list.tail
		list.tail.next = n
		list.tail = list.tail.next
	}
	list.length++
}

func (list *List) LPush(val *GObj) {
	n := &Node{
		Val: val,
	}
	if list.head == nil {
		list.head = n
		list.tail = n
	} else {
		n.next = list.head
		list.head.prev = n
		list.head = n
	}
	list.length++
}

func (list *List) DelNode(n *Node) {
	if n == nil {
		return
	}
	if list.head == n {
		if n.next != nil {
			n.next.prev = nil
		}
		list.head = n.next
		n.next = nil
	} else if list.tail == n {
		if n.prev != nil {
			n.prev.next = nil
		}
		list.tail = n.prev
		n.prev = nil
	} else {
		if n.prev != nil {
			n.prev.next = n.next
		}
		if n.next != nil {
			n.next.prev = n.prev
		}
		n.prev = nil
		n.next = nil
	}
	list.length--
}

func (list *List) Delete(val *GObj) {
	list.DelNode(list.Find(val))
}
