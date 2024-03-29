package skiplist

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInsertAndSearch(t *testing.T) {
	skiplist := NewSkipList(0.5, 16)

	for i := 1; i < 10; i++ {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		/* Try searching before insert */
		node := skiplist.Search(k)
		require.Nil(t, node)

		/* Validate skiplist properties after insert */
		require.NoError(t, skiplist.Insert(k, v))
		require.NoError(t, skiplist.isValid(t))
		require.True(t, skiplist.isSorted(t))

		/* Try searching after insert */
		node = skiplist.Search(k)
		require.Equal(t, k, node.key)
		require.Equal(t, v, node.val)
	}

	/* Insert into a pre-existing node and check if val has changed correctly */
	existingKey, newVal := []byte("key5"), []byte("val6439")
	require.NoError(t, skiplist.Insert(existingKey, newVal))
	require.NoError(t, skiplist.isValid(t))
	require.True(t, skiplist.isSorted(t))
	node := skiplist.Search(existingKey)
	require.Equal(t, existingKey, node.key)
	require.Equal(t, newVal, node.val)
}

func TestDelete(t *testing.T) {
	skiplist := NewSkipList(0.5, 16)

	err := skiplist.Delete([]byte("key22"))
	require.ErrorIs(t, ErrKeyDoesNotExist, err)

	for i := 1; i < 10; i++ {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		err := skiplist.Insert(k, v)
		require.NoError(t, err)
	}

	randomIdxs := []int{7, 2, 4, 1, 3, 5, 9, 6, 8}
	for i := 0; i < len(randomIdxs); i++ {
		randomIdx := randomIdxs[i]
		k := []byte(fmt.Sprintf("key%d", randomIdx))
		err := skiplist.Delete(k)
		require.NoError(t, err)
		require.NoError(t, skiplist.isValid(t))
		require.True(t, skiplist.isSorted(t))
	}

}

func (sl *SkipList) isSorted(t *testing.T) bool {
	t.Helper()
	for curNode, prevNode := sl.head, sl.head; curNode != sl.nil; prevNode, curNode = curNode, curNode.forward[1] {
		if curNode != prevNode && sl.compareKey(curNode, prevNode) < 0 {
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
