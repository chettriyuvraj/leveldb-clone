package skiplist

import (
	"errors"
)

var ErrInvalidNodeLevel = errors.New("invalid node level")
var ErrKeyDoesNotExist = errors.New("key does not exist")

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
	skiplist := SkipList{head: NewNode(nil, nil), nil: NewNode(nil, nil), level: 1, p: p, maxLevel: maxLevel}
	skiplist.head.forward = append(skiplist.head.forward, skiplist.nil)
	return &skiplist
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

func (sl *SkipList) Search(key []byte) *Node {
	/* First search for the insertion spot using a dummy search node with the same key*/
	node, dummySearchNode := sl.head, NewNode(key, nil)
	for i := sl.level; i > 0; i-- {
		for sl.compareKey(node.forward[i], dummySearchNode) < 0 { /* As long as node's key is less than forward[i] */
			node = node.forward[i]
		}
	}

	nodeAhead := node.forward[1]
	if sl.compareKey(dummySearchNode, nodeAhead) == 0 {
		return nodeAhead
	}
	return nil
}

/*
- Returns closest key node if searched key doesn't exist
- Note: Returns 'right' value instead of 'left' value E.g. ["key2", "key4"] -> A search for "key3" will yield "key4"
*/
func (sl *SkipList) SearchClosest(key []byte) *Node {
	/* First search for the insertion spot using a dummy search node with the same key*/
	node, dummySearchNode := sl.head, NewNode(key, nil)
	for i := sl.level; i > 0; i-- {
		for sl.compareKey(node.forward[i], dummySearchNode) < 0 { /* As long as node's key is less than forward[i] */
			node = node.forward[i]
		}
	}

	nodeAhead := node.forward[1]
	if sl.compareKey(dummySearchNode, nodeAhead) == 0 {
		return nodeAhead
	}

	if nodeAhead != sl.nil {
		return nodeAhead
	}

	return nil
}

func (sl *SkipList) Insert(key, val []byte) error {
	/* Init */
	node, newNode := sl.head, NewNode(key, val)
	updateList := []*Node{NewNode(nil, nil)} /* update list is 1-indexed, so 0th node is a dummy node */
	for i := 1; i <= sl.maxLevel; i++ {
		updateList = append(updateList, NewNode(nil, nil))
	}

	/* First search for the insertion spot */
	for i := sl.level; i > 0; i-- {
		for sl.compareKey(node.forward[i], newNode) < 0 { /* As long as node.forward[i] is less than search key */
			node = node.forward[i]
		}
		updateList[i] = node
	}

	/* If key already exists, simply update value */
	nodeAhead := node.forward[1]
	if sl.compareKey(newNode, nodeAhead) == 0 {
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
	for i := 1; i <= randomLevel; i++ {
		newNode.forward = append(newNode.forward, updateList[i].forward[i])
		updateList[i].forward[i] = newNode
	}

	return nil
}

func (sl *SkipList) Delete(key []byte) error {
	/* Init */
	node, dummySearchNode := sl.head, NewNode(key, nil)
	updateList := []*Node{NewNode(nil, nil)} /* update list is 1-indexed, so 0th node is a dummy node */
	for i := 1; i <= sl.maxLevel; i++ {
		updateList = append(updateList, NewNode(nil, nil))
	}

	/* First search for the insertion spot */
	for i := sl.level; i > 0; i-- {
		for sl.compareKey(node.forward[i], dummySearchNode) < 0 { /* As long as node.forward[i] is less than search key */
			node = node.forward[i]
		}
		updateList[i] = node
	}

	/* If key doesn't exists */
	nodeAhead := node.forward[1]
	if sl.compareKey(dummySearchNode, nodeAhead) != 0 {
		return ErrKeyDoesNotExist
	}

	/* Else delete node and modify level if level changed */
	for i := 1; i <= sl.level; i++ {
		if updateList[i].forward[i] != nodeAhead {
			break
		}
		updateList[i].forward[i] = nodeAhead.forward[i]
	}

	for sl.level > 1 && sl.head.forward[sl.level] == sl.nil {
		sl.level--
	}

	return nil

}
