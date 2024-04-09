package skiplist

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strings"
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

func (sl *SkipList) String() string {
	/* Construct a map illustrating the skiplist in order from left to right */
	slInSequence := map[*Node]int{}
	for i, curNode := 0, sl.head; ; curNode, i = curNode.forward[1], i+1 {
		slInSequence[curNode] = i
		if curNode == sl.nil {
			break
		}
	}

	/* Note:
	- Defining a func to print each level, then using it to print each level i.e. 5th level first
	- Print of Key:Val will start getting disoriented if length of key/val gets longer
	- Level 0 indicates key is to be printed, Level -1 indicates val is to be printed
	*/
	drawLevel := func(level int, slInSequence map[*Node]int) string {
		distanceBetweenAdjLevels := 10
		var sb strings.Builder
		if level > 0 {
			sb.WriteString(fmt.Sprintf("[%d]", level))
		} else {
			sb.WriteString("[ ]")
		}

		curNode, prevNode := sl.head, sl.head
		for {
			/* Printing each level*/
			if curNode != sl.head {
				levelsBetweenCurNodeAndPrevNode := slInSequence[curNode] - slInSequence[prevNode]
				for i := 0; i < levelsBetweenCurNodeAndPrevNode; i++ {
					if i > 0 {
						sb.WriteString("---") /* Three dashes extra for every time we don't have a node on this level */
					}
					for j := 0; j < distanceBetweenAdjLevels; j++ {
						if level > 0 {
							sb.WriteString("-")
						} else {
							sb.WriteString(" ")
						}
					}
				}
				if level == 0 {
					sb.WriteString(fmt.Sprintf("(%s)", string(curNode.key)))
				} else if level == -1 {
					sb.WriteString(fmt.Sprintf("(%s)", string(curNode.val)))
				} else {
					sb.WriteString(fmt.Sprintf("[%d]", level))
				}
			}

			if curNode == sl.nil {
				break
			}

			if level > 0 {
				prevNode, curNode = curNode, curNode.forward[level]
			} else {
				prevNode, curNode = curNode, curNode.forward[1]
			}

		}
		return sb.String()
	}

	/* Draw the skip list level-by-level i.e. entire 5th level first, then 4th, then 3rd ...*/
	var sb strings.Builder
	for curLevel := sl.level; curLevel >= -1; curLevel-- {
		sb.WriteString(drawLevel(curLevel, slInSequence))
		sb.WriteString("\n")
	}

	return sb.String()
}

func (sl *SkipList) FirstKey() []byte {
	return sl.head.forward[1].key
}

func (sl *SkipList) Nil() *Node {
	return sl.nil
}

func (node *Node) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\n\nKey: %s", string(node.key)))
	for i := 1; i <= node.Level(); i++ {
		sb.WriteString(fmt.Sprintf("\nLevel %d: %s\n", i, string(node.forward[i].key)))
	}
	return sb.String()
}

func (node *Node) Key() []byte {
	if node == nil {
		return nil
	}
	return node.key
}

func (node *Node) Val() []byte {
	if node == nil {
		return nil
	}
	return node.val
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
- Note: Returns nil instead of returning the skiplist nil node
*/
func (node *Node) GetAdjacent() *Node {
	if node == nil || len(node.forward) <= 1 || node.forward[1].key == nil { /* Final condition is a test for skip list nil node - we'll return nil instead of returning nil node */
		return nil
	}
	return node.forward[1]
}

/*
- Assuming first level is always a dummy level
*/
func (node *Node) Level() int {
	return len(node.forward) - 1
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

/*
- Compares skiplist nodes WRT key (one node must be 'newNode' that is being searched for/deleted/inserted in skip list)
- We use a separate method as this handles comparison for 'nil' element in skiplist gracefully - use this for comparison ops in search/insert/delete
*/
func (sl *SkipList) compareKey(n1, n2 *Node) int {
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
