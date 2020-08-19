/*
	This file is an implement of skiplist in golang
	Translated code from levelDB

	Author: windleaves
	Date: 2020-07-20 19:11:00
	Mail: windleaves@xiyoulinux.org

	Copyleft by windleaves, all wrongs reserved .
*/

package main

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"
)

const (
	MaxHeight       = 12
	TossCoinPerhaps = 0.5
	PerhapsCompare  = TossCoinPerhaps * 0xFFFF
)

type Key []byte
type Value []byte

type Pair struct {
	K Key
	V Value
}

// Node 是skiplist的基础构成元素
type Node struct {
	kv   Pair
	next []*Node
}

func (n *Node) Next(nth int) *Node {
	return n.next[nth]
}

func (n *Node) SetNext(nth int, x *Node) {
	n.next[nth] = x
}

type SkipListIterator interface {
	Valid() bool
	GetKV() Pair
	Next()
	Prev()
	Seek(Key)
	SeekToFirst()
	SeekToLast()
	Close()
}

type SkipListOperator interface {
	NewIterator() SkipListIterator
	Insert(Pair)
	InsertKV(k, v []byte)
	InsertKVString(k, v string)
	Contains(Key) (Pair, bool)
}

type skipIterWrap struct {
	list *skipList
	node *Node
}

func (sw *skipIterWrap) Valid() bool {
	return sw.node != nil
}

func (sw *skipIterWrap) GetKV() Pair {
	if !sw.Valid() {
		panic("iterator not valid")
	}
	return sw.node.kv
}

func (sw *skipIterWrap) Next() {
	if !sw.Valid() {
		panic("iterator not valid")
	}
	// 从0level走肯定是能够走到下一个节点的
	sw.node = sw.node.Next(0)
}

func (sw *skipIterWrap) Prev() {
	if !sw.Valid() {
		panic("iterator not valid")
	}
	sw.node = sw.list.findLessThan(sw.node.kv.K)
	if sw.node == sw.list.head {
		sw.node = nil
	}
}

func (sw *skipIterWrap) Seek(k Key) {
	sw.node = sw.list.findGreaterOrEqual(k, nil)
}

func (sw *skipIterWrap) SeekToFirst() {
	sw.node = sw.list.head.Next(0)
}

func (sw *skipIterWrap) SeekToLast() {
	sw.node = sw.list.findLast()
	if sw.node == sw.list.head {
		sw.node = nil
	}
}

func (sw *skipIterWrap) Close() {
	sw.list.decRef()
}

// skipList 本体
type skipList struct {
	max_height int
	head       *Node
	comparator func(k1, k2 interface{}) int
	refCount   int64
}

func NewSkipList(comp func(k1, k2 interface{}) int) SkipListOperator {
	rand.Seed(time.Now().UnixNano())
	skipList := &skipList{max_height: 1, comparator: comp}
	skipList.head = skipList.NewNode(Pair{}, MaxHeight)
	skipList.max_height = 1
	// 初始化第一个节点，即索引节点
	for i := 0; i < MaxHeight; i++ {
		skipList.head.SetNext(i, nil)
	}
	return skipList
}

// 投硬币，决定层数
/* Return a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and _max_level
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned .
 */
func (s *skipList) randomHeight() int {
	rand.Seed(time.Now().UnixNano())
	level := 1
	for level < MaxHeight {
		randn := rand.Intn(math.MaxInt64) & 0xFFFF
		if float64(randn) < PerhapsCompare {
			level += 1
		} else {
			break
		}
	}

	return level
}

func (s *skipList) equal(k1, k2 Key) bool {
	return s.comparator(k1, k2) == 0
}

// key 是否 在Node 之后 , 按照递增，key在Node之后，则key > node.key
func (s *skipList) keyIsAfterNode(k Key, n *Node) bool {
	return (n != nil) && (s.comparator(n.kv.K, k) < 0)
}

// 获取当前最大高度
func (s *skipList) getMaxHeight() int {
	return s.max_height
}

// 找到大于等于 k 的节点
func (s *skipList) findGreaterOrEqual(k Key, prev []*Node) *Node {
	h := s.head
	level := s.getMaxHeight() - 1
	for {
		next := h.Next(level)
		// 如果待查找的k在head的有侧，则表示k较大，需要继续，减小level
		if s.keyIsAfterNode(k, next) {
			h = next
		} else {
			// 这里在insert时候会用到，表示，insert时候查找插入点，此时该层的next应当是h
			if prev != nil {
				prev[level] = h
			}
			if level == 0 {
				return next
			} else {
				// 这里退化为层级的顺序查找索引
				level--
			}
		}
	}
}

func (s *skipList) findLessThan(k Key) *Node {
	h := s.head
	level := s.getMaxHeight() - 1
	for {
		// 保证，上一次的key是小于当前的
		if h != s.head && s.comparator(h.kv.K, k) >= 0 {
			panic("comparator not success")
		}
		next := h.Next(level)
		if next == nil || s.comparator(next.kv.K, k) >= 0 {
			// 找到最后一个了
			if level == 0 {
				return h
			}
			// 从h下一层继续找
			level--
		} else {
			// 还没找到最后一个，继续从当前的next 开始找
			h = next
		}
	}
}

// 找到最后一个节点，只需要将next持续向前直到level=0停止
func (s *skipList) findLast() *Node {
	level := s.getMaxHeight() - 1
	h := s.head
	for {
		next := h.Next(level)
		if next == nil {
			if level == 0 {
				return h
			}
			// 从h下一层继续查找
			level--
		} else {
			h = next
		}
	}
}

func (s *skipList) incRef() {
	atomic.AddInt64(&s.refCount, 1)
}

func (s *skipList) decRef() {
	atomic.AddInt64(&s.refCount, -1)
}

func (s *skipList) NewIterator() SkipListIterator {
	iter := &skipIterWrap{list: s, node: nil}
	s.incRef()
	return iter
}

func (s *skipList) Insert(p Pair) {
	// 插入时，寻找待插入点的所有前驱Node
	prev := make([]*Node, MaxHeight)
	x := s.findGreaterOrEqual(p.K, prev)
	// 当前的实现不允许相等的key插入
	if x != nil && s.equal(p.K, x.kv.K) {
		panic("not allow duplicated key")
	}

	// 获取一个随机高度
	height := s.randomHeight()
	currMaxHeight := s.getMaxHeight()
	if height > currMaxHeight {
		for i := currMaxHeight; i < height; i++ {
			prev[i] = s.head
		}
		s.max_height = height
	}

	x = s.NewNode(p, height)

	for i := 0; i < height; i++ {
		x.SetNext(i, prev[i].Next(i))
		prev[i].SetNext(i, x)
	}

}

func (s *skipList) InsertKV(k, v []byte) {
	s.Insert(Pair{K: k, V: v})
}

func (s *skipList) InsertKVString(k, v string) {
	s.Insert(Pair{K: Key(k), V: Value(v)})
}

func (s *skipList) Contains(k Key) (Pair, bool) {
	n := s.findGreaterOrEqual(k, nil)
	if n != nil && s.equal(k, n.kv.K) {
		return n.kv, true
	}
	return Pair{}, false
}

func (s *skipList) NewNode(kv Pair, height int) *Node {
	n := &Node{}
	n.kv = kv
	n.next = make([]*Node, height)
	return n
}

func comp(k1, k2 interface{}) int {
	s1, ok := k1.(Key)
	if !ok {
		s := fmt.Sprintf("%T", s1)
		panic("not support compare key type, only support string " + s)
	}
	s2, ok := k2.(Key)
	if !ok {
		s := fmt.Sprintf("%T", s1)
		panic("not support compare key type, only support string " + s)
	}
	return strings.Compare(string(s1), string(s2))
}

func main() {
	sl := NewSkipList(comp)
	sl.InsertKVString("hello", "world")
	sl.InsertKVString("wahaha", "cacaca")

	iter := sl.NewIterator()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		kv := iter.GetKV()
		fmt.Println("Key:", string(kv.K), "Value:", string(kv.V))
	}

	iter.Close()
}
