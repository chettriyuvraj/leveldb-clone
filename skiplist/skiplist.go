package skiplist

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strings"
)

var ErrInvalidNodeLevel = errors.New("invalid node level")

type Node struct {
	key, val []byte
	forward  []*Node
}

type SkipList struct {
	head     *Node
	level    int
	nil      *Node
	p        float64
	maxLevel int
}

func NewSkipList(p float64, maxLevel int) *SkipList {
	skiplist := SkipList{head: NewNode([]byte{}, []byte{}), nil: NewNode([]byte{}, []byte{}), level: 1, p: p, maxLevel: maxLevel}
	skiplist.head.forward = append(skiplist.head.forward, skiplist.nil)
	return &skiplist
}

func (sl *SkipList) String() string {
	var sb strings.Builder
	sb.WriteString("Head: ")
	sb.WriteString(sl.head.String())
	for curNode := sl.head.forward[1]; curNode != sl.nil; curNode = curNode.forward[1] {
		sb.WriteString(curNode.String())
	}
	return sb.String()
}

func (node *Node) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\n\nKey: %s", string(node.key)))
	for i := 1; i <= node.getLevel(); i++ {
		sb.WriteString(fmt.Sprintf("\nLevel %d: %s\n", i, string(node.forward[i].key)))
	}
	return sb.String()
}

/*
- Initialize new node using this function ONLY
- Returns new node after appending a dummy level to 0th index
*/
func NewNode(key, val []byte) *Node {
	node := &Node{key: key, val: val}
	node.forward = append(node.forward, &Node{})
	return node
}

/*
- Assuming first level is always a dummy level
*/
func (node *Node) getLevel() int {
	return len(node.forward) - 1
}

func (sl *SkipList) Search(key []byte) *Node {
	/* First search for the insertion spot using a dummy search node with the same key*/
	node, dummySearchNode := sl.head, NewNode(key, []byte{})
	for i := sl.level; i > 0; i-- {
		for sl.compare(node.forward[i], dummySearchNode) < 0 { /* As long as node's key is less than forward[i] */
			node = node.forward[i]
		}
	}

	nodeAhead := node.forward[1]
	if sl.compare(dummySearchNode, nodeAhead) == 0 {
		return nodeAhead
	}
	return nil
}

func (sl *SkipList) Insert(key, val []byte) error {
	/* Init */
	node, newNode := sl.head, NewNode(key, val)
	updateList := []*Node{NewNode([]byte{}, []byte{})} /* update list is 1-indexed, so 0th node is a dummy node */
	for i := 1; i <= sl.maxLevel; i++ {
		updateList = append(updateList, NewNode([]byte{}, []byte{}))
	}

	/* First search for the insertion spot */
	for i := sl.level; i > 0; i-- {
		for sl.compare(node.forward[i], newNode) < 0 { /* As long as node.forward[i] is less than search key */
			node = node.forward[i]
		}
		updateList[i] = node
	}

	/* If key already exists, simply update value */
	nodeAhead := node.forward[1]
	if sl.compare(newNode, nodeAhead) == 0 {
		nodeAhead.val = val
		return nil
	}

	/* Else append new node with random level */
	randomLevel := sl.randomLevel()
	if randomLevel > sl.level {
		for i := sl.level + 1; i <= randomLevel; i++ {
			updateList[i] = sl.head
			sl.head.forward = append(sl.head.forward, sl.nil)
		}
		sl.level = randomLevel
	}
	for i := 1; i <= sl.level; i++ {
		newNode.forward = append(newNode.forward, updateList[i].forward[i])
		updateList[i].forward[i] = newNode
	}

	return nil
}

/*
- Handles comparison for 'nil' element in skiplist gracefully - use this for comparison ops in search/insert/delete
*/
func (sl *SkipList) compare(n1, n2 *Node) int {
	if n1 == n2 {
		return 0
	}

	if n1 == sl.nil {
		return 1
	}

	if n2 == sl.nil {
		return -1
	}

	return bytes.Compare(n1.key, n2.key)
}

func (sl *SkipList) randomLevel() int {
	lvl := 1
	for rand.Float64() < sl.p && lvl < sl.maxLevel {
		lvl++
	}
	return lvl
}
