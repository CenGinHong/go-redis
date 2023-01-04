package main

import (
	"errors"
	"math"
	"math/rand"
)

const (
	INIT_SIZE    int64 = 8
	FORCE_RATIO  int64 = 2
	GROW_RATIO   int64 = 2
	DEAFULT_STEP int   = 1
)

var (
	ErrEP = errors.New("expand error")
	ErrEX = errors.New("key exists error")
	ErrNK = errors.New("key doesnt exist error")
)

type Entry struct {
	Key  *GObj
	Val  *GObj
	next *Entry
}

type htable struct {
	table []*Entry
	size  int64 // 这里是指slot的数量，因为用的是拉链法，所以会有used > size的情况
	mask  int64 // 这里是size - 1, 使用位运算来取代取模
	used  int64
}

type DictType struct {
	HashFunc  func(key *GObj) int64
	EqualFunc func(k1, k2 *GObj) bool // 比对key是否相等
}

type Dict struct {
	DictType
	hts       [2]*htable
	rehashIdx int64
	// 没有destructor,go本身就是gc语言
	// iterators
}

func DictCreate(dictType DictType) *Dict {
    dict := &Dict {
		DictType: dictType,
		rehashIdx: -1,
	}
	// 这里暂时未初始化两个htable
	return dict
}

// isRehashing 返回是否在rehash过程中
func (d *Dict) isRehashing() bool {
	return d.rehashIdx != -1
}

func (d *Dict) rehashStep() {
	d.reshash(DEAFULT_STEP)
}

func (d *Dict) reshash(step int) {
	for step > 0 {
		// 已经rehash完成
		if d.hts[0].used == 0 {
			d.hts[0] = d.hts[1]
			d.hts[1] = nil
			d.rehashIdx = -1
			return
		}
		// 找到第一个非空的slot
		for d.hts[0].table[d.rehashIdx] == nil {
			d.rehashIdx += 1
		}
		entry := d.hts[0].table[d.rehashIdx]
		for entry != nil {
			ne := entry.next
			// 这里就是取模，11 % 4 等价于 11 & 3 ，
			// 仅当除数是2的n次方是成立
			idx := d.HashFunc(entry.Key) & d.hts[1].mask
			// 头插
			entry.next = d.hts[1].table[idx]
			d.hts[1].table[idx] = entry
			d.hts[0].used--
			d.hts[1].used++
			entry = ne
		}
		// 清掉ht的这个slot
		d.hts[0].table[d.rehashIdx] = nil
		d.rehashIdx++
		step--
	}
}

func (d *Dict) expandIfNeeded() error {
	// 在rehash过程中，不必扩容
	if d.isRehashing() {
		return nil
	}
	// 初始化桶
	if d.hts[0] == nil {
		return d.expand(INIT_SIZE)
	}
	// 桶超过负载因子
	if d.hts[0].used > d.hts[0].size && d.hts[0].used/d.hts[0].size > FORCE_RATIO {
		return d.expand(d.hts[0].size * GROW_RATIO)
	}
	return nil
}

func (d *Dict) expand(size int64) error {
	// 往上找最近的2的次方数，
	size = nextPower(size)
	// 如果本身已经在rehash中（expandIfNeed保证了不应该处于该过程），
	// 或者传进来的size比现有的size小
	if d.isRehashing() || d.hts[0] != nil && d.hts[0].size >= size {
		return ErrEP
	}
	ht := htable{
		size:  size,
		mask:  size - 1,
		table: make([]*Entry, size),
		used:  0,
	}
	// 如果没有初始化，在这里完成初始化，赋桶直接返回
	if d.hts[0] == nil {
		d.hts[0] = &ht
		d.rehashIdx = -1
		return nil
	}
	// 开始rehash
	d.hts[1] = &ht
	d.rehashIdx = 0
	return nil
}

func nextPower(size int64) int64 {
	for i := INIT_SIZE; i < math.MaxInt64; i *= 2 {
		if i >= size {
			return i
		}
	}
	return -1
}

// keyIndex key应该放在桶的哪一个slot位置，如果已经存在相同的key返回-1
func (d *Dict) keyIndex(key *GObj) int64 {
	// 可能还没初始化
	if err := d.expandIfNeeded(); err != nil {
		return -1
	}
	// 得到hash值
	h := d.HashFunc(key)
	var idx int64
	// 看这个key是否存在，因为不确定是在哪一个桶，都需要看
	for i := 0; i <= 1; i++ {
		idx = h & d.hts[i].mask
		e := d.hts[i].table[idx]
		for e != nil {
			if d.EqualFunc(e.Key, key) {
				return -1
			}
			e = e.next
		}
		// 如果不在rehash，可以不用看第二个桶了
		if !d.isRehashing() {
			break
		}
	}
	return idx
}

// addRaw 若存在返回nil，不存在返回一个新建的entry
func (d *Dict) addRaw(key *GObj) *Entry {
	if d.isRehashing() {
		d.rehashStep()
	}
	idx := d.keyIndex(key)
	// key已经存在
	if idx == -1 {
		return nil
	}
	var ht *htable
	if d.isRehashing() {
		ht = d.hts[1]
	} else {
		ht = d.hts[0]
	}
	e := Entry{
		Key:  key,
		next: ht.table[idx],
	}
	key.IncrRefCount()
	ht.table[idx] = &e
	ht.used += 1
	return &e
}

// add 新增一则key-value,如果key已经存在返回错误
// 要新开一个add方法而不是复用put是因为需要不同记录GObj的ref
func (d *Dict) add(key, val *GObj) error {
	// 返回一个entry,如果该key已经存在则返回一个空entry
	entry := d.addRaw(key)
	if entry == nil {
		return ErrEX
	}
	entry.Val = val
	val.IncrRefCount()
	return nil
}

// Set 找到entry并把val引用值修改
func (d *Dict) Set(key, val *GObj) {
	// 找有没有存在
	entry := d.Find(key)
	// 本身不存在
	if entry == nil {
		d.add(key, val)
	} else {
		// 已存在则修改
		entry.Val.DecrRefCount()
		entry.Val = val
		val.IncrRefCount()
	}
}

func freeEntry(e *Entry) {
	e.Key.DecrRefCount()
	e.Val.DecrRefCount()
}

func (d *Dict) Delete(key *GObj) error {
	if d.hts[0] == nil {
		return ErrNK
	}
	if d.isRehashing() {
		d.rehashStep()
	}
	h := d.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & d.hts[i].mask
		e := d.hts[i].table[idx]
		var prev *Entry
		for e != nil {
			if d.EqualFunc(e.Key, key) {
				if prev == nil {
					d.hts[i].table[idx] = e.next
				} else {
					prev.next = e.next
				}
				freeEntry(e)
				return nil
			}
			prev = e
			e = e.next
		}
		if !d.isRehashing() {
			break
		}
	}
	// key不存在
	return ErrNK
}

// Find 找key对应的键值对,没有则返回nil
func (d *Dict) Find(key *GObj) *Entry {
	if d.hts[0] == nil {
		return nil
	}
	if d.isRehashing() {
		d.rehashStep()
	}
	h := d.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & d.hts[i].mask
		e := d.hts[i].table[idx]
		for e != nil {
			if d.EqualFunc(e.Key, key) {
				return e
			}
			e = e.next
		}
		if !d.isRehashing() {
			break
		}
	}
	return nil
}

// Get 获取key对应的val
func (dict *Dict) Get(key *GObj) *GObj {
	entry := dict.Find(key)
	if entry == nil {
		return nil
	}
	return entry.Val
}

// RandomGet 随机返回一个entry
func (d *Dict) RandomGet() *Entry {
	if d.hts[0] == nil {
		return nil
	}
	if d.isRehashing() {
		d.rehashStep()
	}
	if d.hts[0].used == 0 {
		return nil
	}
	// 在判断因为rehashStep状态可能已经改变
	// 选取哪个桶
	t := 0
	if d.isRehashing() {
		// 根据两个桶的数量平均化随机到哪一个桶
		r := rand.Int63n(d.hts[0].used + d.hts[1].used)
		if r >= d.hts[0].used {
			t = 1
		}
	}
	idx := rand.Int63n(d.hts[t].size)
	for cnt := int64(0); d.hts[t].table[idx] == nil && cnt < d.hts[t].used; cnt++ {
		idx = rand.Int63n(d.hts[t].size)
	}
	// 仍然没有随机到不为空的槽
	if d.hts[t].table[idx] == nil {
		// 顺序遍历一次
		for i := int64(0); i < d.hts[t].size && d.hts[t] == nil; i++ {
		}
	}
	// 求出链长
	listLen := int64(0)
	p := d.hts[t].table[idx]
	for p != nil {
		listLen++
		p = p.next
	}
	// 随机定位到
	listIdx := rand.Int63n(listLen)
	p = d.hts[t].table[idx]
	for i := int64(0); i < listIdx; i++ {
		p = p.next
	}
	return p
}
