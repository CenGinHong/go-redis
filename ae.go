package main

import (
	"log"
	"time"

	"golang.org/x/sys/unix"
)

type FeType int

// AE事件
const (
	AE_READABLE FeType = 1
	AE_WRITABLE FeType = 2
)

type TeType int

const (
	AE_NORMAL TeType = 1 // 每个一段时间重复一次
	AE_ONCE   TeType = 2 // 只能执行一次
)

type FileProc func(loop *AeLoop, fd int, extra interface{})
type TimeProc func(loop *AeLoop, id int, extra interface{})

type AeFileEvent struct {
	fd    int
	mask  FeType
	proc  FileProc
	extra interface{}
}

type AeTimeEvent struct {
	id       int
	mask     TeType // 源码用的与运算所以用mask，这里只用简单的比较
	when     int64  // ms 发生时间点
	interval int64  // ms 发生间隔
	proc     TimeProc
	extra    interface{}
	next     *AeTimeEvent
}

type AeLoop struct {
	FileEvents      map[int]*AeFileEvent // 在client注册和销毁是都要使用到，方便进行添加和销魂
	TimeEvents      *AeTimeEvent         // 使用的链表结构
	fileEventFd     int
	timeEventNextId int
	stop            bool
}

// ae常量到epoll的映射，readable映射EPOLLIN，writeable映射EPOLLOUT
var fe2ep = [3]uint32{0, unix.EPOLLIN, unix.EPOLLOUT}

// 把fd和事件压缩成一个key，例如那个fd可读
func getFeKey(fd int, mask FeType) int {
	if mask == AE_READABLE {
		return fd
	} else {
		return fd * -1
	}
}

// getEpollMask 获得已经绑定的事件
func (loop *AeLoop) getEpollMask(fd int) uint32 {
	var ev uint32
	// 检测A事件可读这个是否已经绑定，没有就添加
	if loop.FileEvents[getFeKey(fd, AE_READABLE)] != nil {
		ev |= fe2ep[AE_READABLE]
	}
	if loop.FileEvents[getFeKey(fd, AE_WRITABLE)] != nil {
		ev |= fe2ep[AE_WRITABLE]
	}
	return ev
}

func (loop *AeLoop) AddFileEvent(fd int, mask FeType, proc FileProc, extra interface{}) {
	// 获取已经绑定的事件
	ev := loop.getEpollMask(fd)
	// 如果已经订阅过,返回
	if ev&fe2ep[mask] != 0 {
		return
	}
	op := unix.EPOLL_CTL_ADD
	if ev != 0 {
		op = unix.EPOLL_CTL_MOD
	}
	// 或操作相当于增加了一种类型操作
	// ev是对应epoll_in,epoll_out
	ev |= fe2ep[mask]
	// 订阅回调事件，第一个fd用的是创建的epollfd
	if err := unix.EpollCtl(loop.fileEventFd, op, fd,
		&unix.EpollEvent{Fd: int32(fd), Events: ev}); err != nil {
		log.Printf("epoll ctr err: %v\n", err)
		return
	}
	// 创建ae事件
	fe := AeFileEvent{
		fd:    fd,
		mask:  mask, // readable or writeable
		proc:  proc, // 事件的处理
		extra: extra,
	}
	loop.FileEvents[getFeKey(fd, mask)] = &fe
	log.Printf("ae add file event fd:%v, mask:%v\n", fd, mask)
}

func (loop *AeLoop) RemoveFileEvent(fd int, mask FeType) {
	op := unix.EPOLL_CTL_DEL
	ev := loop.getEpollMask(fd)
	// 相当于摘除操作
	ev &= ^fe2ep[mask]
	if ev != 0 {
		op = unix.EPOLL_CTL_MOD
	}
	if err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{
		Fd:     int32(fd),
		Events: ev,
	}); err != nil {
		log.Printf("epoll del err: %v\n", err)
	}
	delete(loop.FileEvents, getFeKey(fd, mask))
	log.Printf("ae remove file event fd:%v, mask:%v\n", fd, mask)
}

// GetMsTime 获取当前时间
func GetMsTime() int64 {
	return time.Now().UnixNano() / 1e6
}

func (loop *AeLoop) AddTimeEvent(mask TeType, interval int64, proc TimeProc, extra interface{}) int {
	id := loop.timeEventNextId
	loop.timeEventNextId++
	te := AeTimeEvent{
		id:       id,
		mask:     mask,
		interval: interval,
		when:     GetMsTime() + interval,
		proc:     proc,
		extra:    extra,
		next:     loop.TimeEvents,
	}
	// 头插入
	loop.TimeEvents = &te
	return id
}

func (loop *AeLoop) RemoveTimeEvent(id int) {
	p := loop.TimeEvents
	var pre *AeTimeEvent
	for p != nil {
		if p.id == id {
			if pre == nil {
				loop.TimeEvents = p.next
			} else {
				pre.next = p.next
			}
			p.next = nil
			break
		}
		pre = p
		p = p.next
	}
}

func AeLoopCreate() (*AeLoop, error) {
	epollFd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &AeLoop{
		FileEvents:      make(map[int]*AeFileEvent),
		fileEventFd:     epollFd,
		timeEventNextId: 1,
		stop:            false,
	}, nil
}

func (loop *AeLoop) nearestTime() int64 {
	var nearest = GetMsTime() + 1000
	p := loop.TimeEvents
	for p != nil {
		if p.when < nearest {
			nearest = p.when
		}
		p = p.next
	}
	return nearest
}

func (loop *AeLoop) AeWait() (tes []*AeTimeEvent, fes []*AeFileEvent) {
	// loop.nearestTime() 求出等待io事件的最长时间，最长不能超过当前时间+1s，最短是10ms
	timeout := loop.nearestTime() - GetMsTime()
	if timeout <= 0 {
		timeout = 10
	}
	// 收集所有的网络事件fd
	var events [128]unix.EpollEvent
	// 等待事件时间不能超过下一个时间事件到来之前
	n, err := unix.EpollWait(loop.fileEventFd, events[:], int(timeout))
	if err != nil {
		log.Printf("epoll wait warning: %v\n", err)
	}
	if n > 0 {
		log.Printf("ae get %v epoll events\n", n)
	}
	// 收集所有file events
	for i := 0; i < n; i++ {
		var mask FeType
		if events[i].Events&unix.EPOLLIN != 0 {
			mask = AE_READABLE
		} else if events[i].Events&unix.EPOLLOUT != 0 {
			mask = AE_WRITABLE
		} else {
			continue
		}
		feKey := getFeKey(int(events[i].Fd), mask)
		if fe := loop.FileEvents[feKey]; fe != nil {
			fes = append(fes, fe)
		}
	}
	// 找出所有到点的事件
	now := GetMsTime()
	p := loop.TimeEvents
	for p != nil {
		if p.when <= now {
			tes = append(tes, p)
		}
		p = p.next
	}
	return
}

func (loop *AeLoop) AeProcess(tes []*AeTimeEvent, fes []*AeFileEvent) {
	for _, te := range tes {
		te.proc(loop, te.id, te.extra)
		// 如果该事件时间只执行一次，马上移除
		if te.mask == AE_ONCE {
			loop.RemoveTimeEvent(te.id)
		} else {
			// 更新下次发生的时间点
			te.when = GetMsTime() + te.interval
		}
	}
	if len(fes) > 0 {
		log.Println("ae is processing file events")
		for _, fe := range fes {
			fe.proc(loop, fe.fd, fe.extra)
		}
	}
}

func (loop *AeLoop) AeMain() {
	for !loop.stop {
		// 收集所有的事件
		tes, fes := loop.AeWait()
		loop.AeProcess(tes, fes)
	}
}
