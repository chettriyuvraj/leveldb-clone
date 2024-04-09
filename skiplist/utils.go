package skiplist

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
)

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

func (sl *SkipList) FirstKey() []byte {
	return sl.head.forward[1].key
}

func (sl *SkipList) Nil() *Node {
	return sl.nil
}

/*
- Compares skiplist nodes WRT key
- Separate method used as this handles comparison for 'nil' element in skiplist - use for comparison in search/insert/delete ops
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
