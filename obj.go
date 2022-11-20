package main

import "strconv"

type GType uint8

const (
	GSTR  GType = 0x00
	GLIST GType = 0x01
	GSET  GType = 0x02
	GZSET GType = 0x03
	GDICT GType = 0x04
)

type GVal any

type GObj struct {
	Type_    GType
	Val_     GVal
	refCount int
}

func (o *GObj) IntVal() int {
	if o.Type_ != GSTR {
		return 0
	}
	val, _ := strconv.Atoi(o.Val_.(string))
	return val
}

func CreateFromInt(val int) *GObj {
	return &GObj{
		Type_:    GSTR,
		Val_:     strconv.Itoa(val),
		refCount: 1,
	}
}

func CreateObject(typ GType, ptr any) *GObj {
	return &GObj{
		Type_:    typ,
		Val_:     ptr,
		refCount: 1,
	}
}

func (o *GObj) IncrRefCount() {
	o.refCount++
}

func (o *GObj) DecrRefCount() {
	o.refCount--
	if o.refCount == 0 {
		// let GC do the woek
		o.Val_ = nil
	}
}
