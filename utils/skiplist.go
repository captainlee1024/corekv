package utils

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"github.com/hardcore-os/corekv/utils/codec"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	source := rand.NewSource(time.Now().UnixNano())

	return &SkipList{
		header: &Element{
			levels: make([]*Element, defaultMaxLevel),
			entry:  nil,
			score:  0,
		},
		rand:     rand.New(source),
		maxLevel: defaultMaxLevel,
		length:   0,
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	// 添加不用判空

	// 拿到链表头
	prevElem := list.header
	// 记录每一层应该插入的位置
	var prevElemHeaders [defaultMaxLevel]*Element
	// 计算当前最大层数
	i := len(prevElem.levels) - 1
	// 计算分数
	score := list.calcScore(data.Key)

	for i >= 0 {
		// 记录每一层开始查找的起始位置
		prevElemHeaders[i] = prevElem
		// 找到每一层应该插入的位置，并记录
		// 进入下一层的条件为data.key > next.entry.key

		//for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			if comp := list.compare(
				list.calcScore(data.Key),
				data.Key,
				next,
			); comp <= 0 {
				if comp == 0 {
					// 如果存在该值，更新并返回
					next.entry = data
					return nil
				}

				// 找到插入点，break
				break

			}
			// 找到合适的插入位置
			// 更新开始时的记录，进入下一层
			prevElem = next
			prevElemHeaders[i] = prevElem



		}

		// i--

		// 用来代替 i-- 可加速查找
		// 当 每一层的prevEmel的下一个节点都是同一个值时，就不用再查找了，跳过该层
		// 如果直到最后一层仍然没有，则，在该位置插入
		// 直到不相等时开始在该层查找
		topLevel := prevElem.levels[i]
		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {
			prevElemHeaders[i] = prevElem
		}
	}

	level := list.randLevel()
	elem := newElement(score, data, level)
	for i := 0; i < level; i++ {
		elem.levels[i] = prevElemHeaders[i].levels[i]
		prevElemHeaders[i].levels[i] = elem
	}

	list.size += data.Size()
	list.length++
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	// 链表是否为空
	if list.length == 0 {
		return nil
	}

	// 拿到链表头
	prevElem := list.header
	// 计算层数
	i := len(list.header.levels) - 1
	for i >= 0 {
		// 查找每一层，进入下一层的条件为，findKey > next.entry.Key
		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			if comp := list.compare(list.calcScore(key), key, next); comp <= 0 {
				if comp == 0 {
					return next.entry
				}

				// findKey > next.entry.Key
				break
			}
			prevElem = next

		}

		//i--

		topLevel := prevElem.levels[i]

		//
		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {
		}
	}

	return nil
}

func (list *SkipList) Close() error {
	return nil
}

/*
这里计算一个分数值，用来加速比较。
举个例子：aabbccddee和 aabbccdeee，如果用 bytes的 compare，需要比较到第8个字符才能算出大小关系
如果引入 hash，对前8位计算出一个分数值，比较起来就会很快了
*/
func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	// 分数相等比较数组
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}

	// 分数不同，比较数组
	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	for i := 0; ; i++ {
		if rand.Intn(2) == 0 {
			return i
		}
	}
}

func (list *SkipList) Size() int64 {
	var size int64
	for next := list.header.levels[0]; next != nil; next = next.levels[0] {
		size += next.entry.Size()
	}
	return size
}
