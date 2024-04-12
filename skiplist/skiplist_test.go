package skiplist

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInsertAndSearch(t *testing.T) {
	const (
		INSERT = iota
		OVERWRITE
	)

	skiplist := NewSkipList(0.5, 16)

	tcs := []struct {
		name int
		k, v []byte
	}{
		{name: INSERT, k: []byte("key1"), v: []byte("val1")},
		{name: INSERT, k: []byte("key2"), v: []byte("val2")},
		{name: INSERT, k: []byte("key3"), v: []byte("val3")},
		{name: OVERWRITE, k: []byte("key3"), v: []byte("val3")},
	}

	for _, tc := range tcs {
		var node *Node
		k, v := tc.k, tc.v

		/* Must not exist before insert, if a new node */
		if tc.name == INSERT {
			node := skiplist.Search(k)
			require.Nil(t, node)
		}

		/* Validate skiplist properties after insert */
		require.NoError(t, skiplist.Insert(k, v, nil))
		require.NoError(t, skiplist.isValid(t))
		require.True(t, skiplist.isSorted(t))

		/* Must exist after insert */
		node = skiplist.Search(k)
		require.Equal(t, k, node.key)
		require.Equal(t, v, node.val)
	}
}

func TestDelete(t *testing.T) {
	skiplist := NewSkipList(0.5, 16)

	/* Deleting a record that doesn't exist */
	err := skiplist.Delete([]byte("key22"))
	require.ErrorIs(t, ErrKeyDoesNotExist, err)

	/* Inserting keys defined as "key1":"val1", "key2:val2" */
	for i := 1; i <= 10; i++ {
		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
		err := skiplist.Insert(k, v, nil)
		require.NoError(t, err)
	}

	/* Picking up random keys from existing ones, deleting them + verifying skip list properties maintained */
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
		if curNode != prevNode && sl.compareKey(curNode, prevNode) <= 0 {
			return false
		}
	}
	return true
}

func (sl *SkipList) isValid(t *testing.T) error {
	t.Helper()
	for curNode := sl.head; curNode != sl.nil; curNode = curNode.forward[1] {
		if err := sl.verifyAllLevels(curNode); err != nil {
			return err
		}
	}
	return nil
}

/*
- For the node belonging a given skiplist, verify that all nodes that the it is pointing to are correct
*/
func (sl *SkipList) verifyAllLevels(node *Node) error {
	for level := 1; level <= node.Level(); level++ {
		if err := sl.verifyLevel(node, level); err != nil {
			return err
		}
	}
	return nil
}

/*
- Verify that out of all nodes from src to dst - only dst is >= level
- For e.g. if node.forward[3] is pointing to 'dst', then between node and dst, dst must be the only node with level >= 3. This is a property of skip lists
*/
func (sl *SkipList) verifyLevel(src *Node, level int) error {
	if src.Level() < level {
		return fmt.Errorf("number of levels: %d in src node (key: %s) is less than the provided level: %d", src.Level(), string(src.key), level)
	}

	dst := src.forward[level]

	if dst != sl.nil && dst.Level() < level {
		return fmt.Errorf("number of levels: %d in dst node (key: %s) is less than the provided level: %d", dst.Level(), string(dst.key), level)
	}

	for curNode := src.forward[1]; curNode != dst; curNode = curNode.forward[1] {
		if curNode.Level() >= level {
			return fmt.Errorf("src node (key: %s); dst node (key: %s); is invalid for provided level: %d; node (key: %s) (level: %d) interrupts in the middle", string(src.key), string(dst.key), level, string(curNode.key), curNode.Level())
		}
	}

	return nil
}
