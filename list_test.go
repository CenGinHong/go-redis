package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestList(t *testing.T) {
	list := ListCreate(ListType{EqualFunc: GStrEqual})
	assert.Equal(t, list.Length(), 0)

	list.Append(CreateObject(GSTR, "1"))
	list.Append(CreateObject(GSTR, "2"))
	list.Append(CreateObject(GSTR, "3"))
	assert.Equal(t, 3, list.Length())
	assert.Equal(t, "1", list.First().Val.Val_.(string))
	assert.Equal(t, "3", list.Last().Val.Val_.(string))

	o := CreateObject(GSTR, "0")
	list.LPush(o)
	assert.Equal(t, 4, list.Length())
	assert.Equal(t, "0", list.First().Val.Val_.(string))

	list.LPush(CreateObject(GSTR, "-1"))
	assert.Equal(t, 5, list.Length())
	n := list.Find(o)
	assert.Equal(t, o, n.Val)

	list.Delete(o)
	assert.Equal(t, 4, list.Length())
	n = list.Find(o)
	assert.Nil(t, n)

}
