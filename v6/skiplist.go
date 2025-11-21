package v6

import (
	"bytes"
	"errors"
	"math"
	_ "unsafe"
)

const (
	MaxHeight   = 16
	PValue			= 0.5
)

var probabilities [MaxHeight]uint32

//go:linkname fastrandUint32 runtime.fastrand
func fastrandUint32() uint32

type Node struct {
	Key		[]byte
	Value	[]byte
	Tower	[]*Node
}

type SkipList struct {
	Head		*Node
	Height	int
}

type Iterator struct {
	current 	*Node
	skiplist	*SkipList
}

func NewSkipList() *SkipList {
	sl := &SkipList{}
	sl.Head = &Node{Tower: make([]*Node, MaxHeight)}
	sl.Height = 1
	return sl
}

func (sl *SkipList) NewIterator() *Iterator {
	return &Iterator{
		current: sl.Head,
		skiplist: sl,
	}
}

func init() {
	probability := 1.0

	for level := 0; level < MaxHeight; level++ {
		probabilities[level] = uint32(probability * float64(math.MaxUint32))
		probability *= PValue
	}
}

// We need a random height for each new node we set in the list
func randomHeight() int {
	seed := fastrandUint32()

	height := 1
	for height < MaxHeight && seed <= probabilities[height] {
		height++
	}

	return height
}

// Returns node with the exact key match
// We search by comparing nodes, if the key is greater, we keep advancing nodes.
// If the key is smaller or equal, we go down a level. Repeat until we find the key.
func (sl *SkipList) search(k []byte) (*Node, [MaxHeight]*Node) {
	var next *Node
	var journey [MaxHeight]*Node

	prev := sl.Head // We start at the head of the list
	for level := sl.Height - 1; level >= 0; level-- {
		for next = prev.Tower[level]; next != nil; next = prev.Tower[level] {
			if bytes.Compare(k, next.Key) <= 0 {
				break
			}
			prev = next
		}
		journey[level] = prev
	}

	if next != nil && bytes.Equal(k, next.Key) {
		return next, journey
	}
	return nil, journey
}

// searchGE finds first key >= target for seeks and range queries
func (sl *SkipList) searchGE(target []byte) *Node {
	var next *Node
	prev := sl.Head
	
	for level := sl.Height - 1; level >= 0; level-- {
		for next = prev.Tower[level]; next != nil; next = prev.Tower[level] {
			if bytes.Compare(next.Key, target) >= 0 {
				break
			}
			prev = next
		}
	}
	
	return next
}

func (sl *SkipList) Find(key []byte) ([]byte, error) {
	found, _ := sl.search(key)

	if found == nil {
		return nil, errors.New("key not found")
	}

	return found.Value, nil
}

func (sl *SkipList) Insert(key []byte, val []byte) {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valCopy := make([]byte, len(val))
	copy(valCopy, val)

	found, journey := sl.search(keyCopy)

	// If it exists update the value of the key
	if found != nil {
		found.Value = valCopy
		return
	}

	height := randomHeight()
	nd := &Node{
		Key: keyCopy,
		Value: valCopy,
		Tower: make([]*Node, height),
	}

	for level := 0; level < height; level++ {
		prev := journey[level]

		// If we extend the height of the tree, prev will
		// be nil since that level didnt exist
		if prev == nil {
			prev = sl.Head
		}

		nd.Tower[level] = prev.Tower[level]
		prev.Tower[level] = nd
	}

	if height > sl.Height {
		sl.Height = height
	}
}

func (sl *SkipList) Delete(key []byte) bool {
	found, journey := sl.search(key)

	if found == nil {
		return false
	}

	for level := 0; level < sl.Height; level++ {
		if journey[level].Tower[level] != found {
			break
		}
		journey[level].Tower[level] = found.Tower[level]
		found.Tower[level] = nil
	}
	found = nil
	sl.shrink()

	return true
}

func (sl *SkipList) shrink() {
	for level := sl.Height - 1; level >= 0; level-- {
		if sl.Head.Tower[level] != nil {
			break
		}
		sl.Height--
	}
}

// Moves th eiterator to the next element
// Returns false when theres no more elements
func (it *Iterator) Next() bool {
	if it.current == nil {
		return false
	}

	it.current = it.current.Tower[0]

	return it.current != nil
}

func (it *Iterator) Valid() bool {
	return it.current != nil && it.current.Key != nil
}

// Returns the key at the iterators position
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.current.Key
}

// Returns the value at the iterators position
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.current.Value
}

func (it *Iterator) Seek(target []byte) bool {
	node := it.skiplist.searchGE(target)
	if node != nil {
		it.current = node
		return true
	}

	it.current = nil
	return false
}
