package skiplist

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInsertAndSearch(t *testing.T) {
	skiplist := NewSkipList(0.5, 16)

	for i := 1; i < 10; i++ {
		k := []byte(fmt.Sprintf("%d", i))
		require.NoError(t, skiplist.Insert(k))
		require.NoError(t, skiplist.isValid(t))
		require.True(t, skiplist.isSorted(t))
	}

}

func (sl *SkipList) isSorted(t *testing.T) bool {
	t.Helper()
	for curNode, prevNode := sl.head, sl.head; curNode != sl.nil; prevNode, curNode = curNode, curNode.forward[1] {
		if curNode != prevNode && sl.compare(curNode, prevNode) < 0 {
			return false
		}
	}
	return true
}

func (sl *SkipList) isValid(t *testing.T) error {
	t.Helper()
	for curNode := sl.head; curNode != sl.nil; curNode = curNode.forward[1] {
		if err := curNode.verifyAllLevels(sl.nil); err != nil {
			return err
		}
	}
	return nil
}

/*
- For the node belonging a given skiplist, verify that all nodes that the it is pointing to are correct
- Nil MUST BE the nil pointer i.e. last pointer of the skiplist
*/
func (node *Node) verifyAllLevels(skipListNilNode *Node) error {
	for level := 1; level <= node.getLevel(); level++ {
		if err := node.verifyLevel(level, skipListNilNode); err != nil {
			return err
		}
	}
	return nil
}

/*
- Verify that out of all nodes from src to dst - only dst is >= level
- Note: Both must be part of the same skip list and src must be behind dst - otherwise behaviour undefined
*/
func (src *Node) verifyLevel(level int, skipListNilNode *Node) error {
	if src.getLevel() < level {
		return fmt.Errorf("number of levels: %d in src node (key: %s) is less than the provided level: %d", src.getLevel(), string(src.key), level)
	}

	dst := src.forward[level]

	if dst != skipListNilNode && dst.getLevel() < level {
		return fmt.Errorf("number of levels: %d in dst node (key: %s) is less than the provided level: %d", dst.getLevel(), string(dst.key), level)
	}

	for curNode := src.forward[1]; curNode != dst; curNode = curNode.forward[1] {
		if curNode.getLevel() >= level {
			return fmt.Errorf("src node (key: %s); dst node (key: %s); is invalid for provided level: %d; node (key: %s) (level: %d) interrupts in the middle", string(src.key), string(dst.key), level, string(curNode.key), curNode.getLevel())
		}
	}

	return nil
}
