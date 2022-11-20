package main

type entry struct {
	key  *GObj
	val  *GObj
	next *entry
}

type htable struct {
	table []*entry
	size  int64
	mask  int64
	used  int64
}

type DictType struct {
	HashFunc  func(key *GObj) int
	EqualFunc func(k1, k2 *GObj) bool
}

type Dict struct {
	DictType
	HTable [2]htable
	rehashIdx int
	// 没有destructor,go本身就是gc语言
	// iterators
}

func DictCreate(dictType DictType) *Dict  {
	var dict Dict
	dict.DictType = dictType
	return &dict
}

func (dict *Dict) RandomGet() (key ,val *GObj)  {
	// TODO
	return nil, nil
}

func (dic *Dict) RemoveKey(key *GObj)  {
	// TODO
}


