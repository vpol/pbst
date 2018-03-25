package pbst

import (
	"bytes"
	"fmt"
	"sync/atomic"
)

func (m *Node) insertRecord(key, value []byte, tree *PBSTree) (*Node, error) {

	index := m.locate(key)
	switch m.GetType() {
	case Node_Node:
		tnode, err := tree.getNode(m.Children[index])

		if err != nil {
			return nil, err
		}

		clone, err := tnode.insertRecord(key, value, tree)

		if err != nil {
			return nil, err
		}


		cnode := m.clone(tree)

		cnode.Children[index] = clone.GetId()
		if len(clone.GetKeys()) > int(tree.GetNodeMax()) {
			key, left, right := clone.split(tree)
			cnode.insert(key, left, right, tree)
		}

		tree.Nodes[cnode.GetId()] = cnode
		return cnode, err

	case Node_Leaf:
		if index > 0 {
			if bytes.Compare(m.Keys[index-1], key) == 0 {
				return nil, ErrKeyExists
			}
		}

		var nnode *Node
		if len(m.GetKeys()) == 0 {
			nnode = m
		} else {
			nnode = m.clone(tree)
		}

		nnode.Keys = append(nnode.Keys[:index],
			append([][]byte{key}, nnode.Keys[index:]...)...)
		nnode.Values = append(nnode.Values[:index],
			append([][]byte{value}, nnode.Values[index:]...)...)
		tree.Nodes[nnode.GetId()] = nnode
		return nnode, nil
	}

	return nil, fmt.Errorf("insert record failed")
}

// Insert key into tree node
func (m *Node) insert(key []byte, leftID int64, rightID int64, tree *PBSTree) (err error) {
	index := m.locate(key)
	if len(m.Keys) == 0 {
		m.Children = append([]int64{leftID}, rightID)
	} else {
		m.Children = append(m.Children[:index+1],
			append([]int64{rightID}, m.Children[index+1:]...)...)
	}
	m.Keys = append(m.Keys[:index], append([][]byte{key}, m.Keys[index:]...)...)
	tree.Nodes[m.GetId()] = m
	return
}

func (m *Node) deleteRecord(key []byte, tree *PBSTree) (*Node, []byte, error) {
	index := m.locate(key)
	var err error
	var nnode *Node
	var newKey []byte
	var clonedNode *Node
	if m.GetType() == Node_Node {
		tnode, err := tree.getNode(m.Children[index])
		clonedNode, newKey, err = tnode.deleteRecord(key, tree)
		if err == nil {
			nnode = m.clone(tree)
			nnode.Children[index] = clonedNode.GetId()
			tmpKey := newKey
			if len(newKey) > 0 {
				if nnode.replace(key, newKey) {
					newKey = []byte{}
				}
			}
			if index == 0 {
				index = 1
			}
			if len(nnode.Keys) > 0 {
				left := nnode.merge(tree, index-1)
				if index == 1 && len(tmpKey) == 0 {
					tt, _ := tree.getNode(nnode.Children[0])
					if tt.GetType() == Node_Leaf {
						if len(tt.Keys) > 0 {
							newKey = tt.Keys[0]
						}
					}
				}
				if left > 0 {
					nnode.Children[index-1] = left
				}
			}
		}
	}
	if m.GetType() == Node_Leaf {
		index -= 1
		if index >= 0 && bytes.Compare(m.Keys[index], key) == 0 {
			nnode = m.clone(tree)
			nnode.Keys = append(nnode.Keys[:index], nnode.Keys[index+1:]...)
			nnode.Values = append(nnode.Values[:index], nnode.Values[index+1:]...)
			if index == 0 && len(nnode.Keys) > 0 {
				newKey = nnode.Keys[0]
			}
		} else {
			return nil, newKey, fmt.Errorf("delete failed")
		}
	}
	tree.Nodes[nnode.GetId()] = nnode
	return nnode, newKey, err
}

// replace delete key
func (m *Node) replace(oldKey, newKey []byte) bool {
	index := m.locate(oldKey) - 1
	if index >= 0 {
		if bytes.Compare(m.Keys[index], oldKey) == 0 {
			m.Keys[index] = newKey
			return true
		}
	}
	return false
}

func (m *Node) locate(key []byte) int {
	i := 0
	size := len(m.Keys)
	for {
		mid := (i + size) / 2
		if i == size {
			break
		}
		if bytes.Compare(m.Keys[mid], key) <= 0 {
			i = mid + 1
		} else {
			size = mid
		}
	}
	return i
}

// clone node
func (m *Node) clone(tree *PBSTree) *Node {
	nnode := tree.newNode(m.GetType())
	nnode.Keys = make([][]byte, len(m.Keys))
	copy(nnode.Keys, m.Keys)
	nnode.Children = make([]int64, len(m.GetChildren()))
	copy(nnode.Children, m.GetChildren())
	nnode.Values = make([][]byte, len(m.GetValues()))
	copy(nnode.Values, m.GetValues())
	atomic.StoreInt32(&m.IsDirt, 1)
	return nnode
}

func (m *Node) isReleaseAble() bool {
	if atomic.LoadInt32(&m.IsDirt) > 0 {
		return true
	}

	return false
}

func (m *Node) merge(tree *PBSTree, index int) int64 {
	left, err := tree.getNode(m.Children[index])
	if err != nil {
		return -1
	}
	right, err := tree.getNode(m.Children[index+1])
	if err != nil {
		return -1
	}
	if len(left.Keys)+len(right.Keys) > int(tree.GetNodeMax()) {
		return -1
	}
	if (len(left.Values) + len(right.Values)) > int(tree.GetLeafMax()) {
		return -1
	}
	leftClone := left.clone(tree)
	m.Children[index] = leftClone.GetId()
	if leftClone.GetType() == Node_Leaf {
		if index == len(m.Keys) {
			m.Children = m.Children[:index]
			m.Keys = m.Keys[:index-1]
		} else {
			m.Children = append(m.Children[:index+1], m.Children[index+2:]...)
			m.Keys = append(m.Keys[:index], m.Keys[index+1:]...)
		}
		// add right to left
		leftClone.Values = append(leftClone.Values, right.Values...)
		leftClone.Keys = append(leftClone.Keys, right.Keys...)
	} else {
		leftClone.Keys = append(leftClone.Keys, append([][]byte{m.Keys[index]}, right.Keys...)...)
		// merge Children
		leftClone.Children = append(leftClone.Children, right.Children...)
		// remove old key
		m.Keys = append(m.Keys[:index], m.Keys[index+1:]...)
		// remove old right node
		m.Children = append(m.Children[:index+1], m.Children[index+2:]...)
		// check size, spilt if over size
		if len(leftClone.Keys) > int(tree.GetNodeMax()) {
			key, left, right := leftClone.split(tree)
			m.insert(key, left, right, tree)
		}
	}
	atomic.StoreInt32(&right.IsDirt, 1)
	atomic.StoreInt32(&left.IsDirt, 1)
	tree.Nodes[right.GetId()] = right
	tree.Nodes[left.GetId()] = left
	tree.Nodes[leftClone.GetId()] = leftClone
	return leftClone.GetId()
}

// update node
func (m *Node) updateRecord(key, value []byte, tree *PBSTree) (*Node, error) {
	index := m.locate(key)
	var nnode *Node
	var clonedNode *Node
	var err error
	if m.GetType() == Node_Node {
		tnode, err := tree.getNode(m.Children[index])
		if err != nil {
			return tnode, err
		}
		clonedNode, err = tnode.updateRecord(key, value, tree)
		if err == nil {
			nnode = m.clone(tree)
			nnode.Children[index] = clonedNode.GetId()
		}
	} else {
		index--
		if index >= 0 {
			if bytes.Compare(m.Keys[index], key) == 0 {
				nnode = m.clone(tree)
				nnode.Values[index] = value
			}
		}
	}
	tree.Nodes[nnode.GetId()] = nnode
	return nnode, err
}

func (m *Node) split(tree *PBSTree) (key []byte, left, right int64) {

	nnode := tree.newNode(m.GetType())

	if nnode.Type == Node_Leaf {
		mid := tree.GetLeafMax() / 2
		length := len(m.GetKeys()) - int(mid)
		nnode.Values = make([][]byte, length)
		copy(nnode.Values, m.GetValues()[mid:])
		nnode.Keys = make([][]byte, length)
		copy(nnode.Keys, m.GetKeys()[mid:])
		key = nnode.Keys[0]
		m.Keys = m.Keys[:mid]
		m.Values = m.Values[:mid]
	} else {
		mid := tree.GetNodeMax() / 2
		key = m.Keys[mid]
		length := len(m.GetKeys()) - int(mid+1)
		nnode.Keys = make([][]byte, length)
		copy(nnode.Keys, m.GetKeys()[mid+1:])
		nnode.Children = m.GetChildren()[mid+1:]
		m.Keys = m.Keys[:mid]
		m.Children = m.Children[:mid+1]
	}

	left = m.GetId()
	right = nnode.GetId()

	tree.Nodes[nnode.GetId()] = nnode
	tree.Nodes[m.GetId()] = m
	return
}

// node search record
func (m *Node) searchRecord(key []byte, tree *PBSTree) ([]byte, error) {
	var value []byte
	index := m.locate(key)
	if m.GetType() == Node_Node {
		tnode, err := tree.getNode(m.Children[index])
		if err != nil {
			return value, err
		}
		return tnode.searchRecord(key, tree)
	} else {
		index--
		if index >= 0 {
			if res := bytes.Compare(m.Keys[index], key); res == 0 {
				return m.Values[index], nil
			}
		}
	}
	return value, fmt.Errorf("%s not found", string(key))
}

func (m *Node) String() string {

	toArray := func(data [][]byte) []string {
		var rst []string
		for _, v := range data {
			rst = append(rst, string(v))
		}
		return rst
	}

	var buf bytes.Buffer
	if m.GetType() == Node_Leaf {
		buf.WriteString(fmt.Sprintf("\t---LeafID--- %d\n", m.GetId()))
		buf.WriteString(fmt.Sprintf("\t\tKey %s\n", toArray(m.GetKeys())))
		buf.WriteString(fmt.Sprintf("\t\tValues %s\n", toArray(m.GetValues())))
	} else {
		buf.WriteString(fmt.Sprintf("\t---NodeID--- %d\n", m.GetId()))
		buf.WriteString(fmt.Sprintf("\t\tKey %s\n", toArray(m.GetKeys())))
		buf.WriteString(fmt.Sprintf("\t\tChildren %d\n", m.GetChildren()))
	}

	return buf.String()
}
