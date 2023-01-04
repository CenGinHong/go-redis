package main

import (
	"fmt"
	"strconv"
)

type GType uint8

const (
	GSTR  GType = 0x00
	GLIST GType = 0x01
	GSET  GType = 0x02
	GZSET GType = 0x03
	GDICT GType = 0x04
)

type GVal interface{}

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

func (o *GObj) StrVal() string {
	if o.Type_ != GSTR {
		return ""
	}
	return o.Val_.(string)
}

func (o *GObj) FloatVal() float64 {
	if o.Type_ != GSTR {
		return 0
	}
	return o.Val_.(float64)
}

func CreateFromInt(val int) *GObj {
	return &GObj{
		Type_:    GSTR,
		Val_:     strconv.Itoa(val),
		refCount: 1,
	}
}

func CreateFromFloat(val float64) *GObj {
	return &GObj{
		Type_:    GSTR,
		Val_:     fmt.Sprintf("%f", val), // float转string
		refCount: 1,
	}
}

func CreateObject(typ GType, ptr interface{}) *GObj {
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
		// let GC do the work
		o.Val_ = nil
	}
}
