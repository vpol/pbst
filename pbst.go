package pbst

import (
	"github.com/gogo/protobuf/proto"
	"sync/atomic"
	"sync"
	"bytes"
	"errors"
	"fmt"
)

var ErrNoData = errors.New("no data")
var ErrKeyExists = errors.New("key exists")

// Btree metadata
type PBSTree struct {
	MetaData
	sync.Mutex
	gcIndex     int64
	dupnodelist map[int64]int
}

const (
	TreeSize = 1024
	LeafSize = 32
	NodeSize = 64
)

// NewBtree create a btree
func NewPBST() *PBSTree {
	return NewPBSTWithSize(LeafSize, NodeSize)
}

// NewBtreeSize create new btree with custom leafsize/nodesize
func NewPBSTWithSize(leafsize int64, nodesize int64) *PBSTree {
	tree := &PBSTree{
		dupnodelist: make(map[int64]int),
		MetaData: MetaData{
			Root:        0,
			TSize:       TreeSize,
			LeafMax:     leafsize,
			NodeMax:     nodesize,
			IndexCursor: 0,
			Nodes:       make([]*Node, TreeSize),
		},
	}

	return tree
}

func (t PBSTree) Save(file string) {
	// t.Marshal(file)
}

func (t *PBSTree) Load(file string) {
	// t.Marshal(file)
}

// Insert can insert record into a btree
func (t *PBSTree) Insert(key, value []byte) (err error) {

	tnode, err := t.getNode(t.GetRoot())
	if err != nil {
		if err != ErrNoData {
			return err
		}

		nnode := t.newNode(Node_Leaf)


		_, err = nnode.insertRecord(key, value, t)
		if err == nil {
			t.Nodes[nnode.GetId()] = nnode
		}
		t.Root = nnode.GetId()
		return err
	}

	clone, err := tnode.insertRecord(key, value, t)

	if err == nil && len(clone.GetKeys()) > int(t.GetNodeMax()) {
		nnode := t.newNode(Node_Node)
		key, left, right := clone.split(t)
		nnode.insert(key, left, right, t)
		t.Nodes[nnode.GetId()] = nnode
		t.Root = nnode.GetId()
	} else {
		t.Root = clone.GetId()
	}

	if err == nil {
		t.Index = t.GetIndexCursor()
	}

	return
}

// Delete can delete record
func (t *PBSTree) Delete(key []byte) (err error) {

	tnode, err := t.getNode(t.GetRoot())
	if err != nil {
		return err
	}

	clonedNode, _, err := tnode.deleteRecord(key, t)
	if err == nil {
		if len(clonedNode.GetKeys()) == 0 {
			if clonedNode.GetType() == Node_Node {
				if len(clonedNode.GetChildren()) > 0 {
					newroot, err := t.getNode(clonedNode.Children[0])
					if err == nil {
						atomic.StoreInt32(&clonedNode.IsDirt, 1)
						t.Nodes[clonedNode.GetId()] = clonedNode
						t.Root = newroot.GetId()
					}
					return err
				}
			}
		}
		t.Root = clonedNode.GetId()
	}

	if err == nil {
		t.Index = t.GetIndexCursor()
	}

	return
}

func (t *PBSTree) Search(key []byte) (value []byte, err error) {

	tnode, err := t.getNode(t.GetRoot())
	if err != nil {
		return []byte{}, err
	}

	value, err = tnode.searchRecord(key, t)

	if err == nil {
		t.Index = t.GetIndexCursor()
	}

	return
}

func (t *PBSTree) Update(key, value []byte) (err error) {

	tnode, err := t.getNode(t.GetRoot())
	if err != nil {
		return err
	}
	clonedNode, err := tnode.updateRecord(key, value, t)

	if err == nil {
		t.Root = clonedNode.GetId()
	}

	if err == nil {
		t.Index = t.GetIndexCursor()
	}

	return
}

// genrate node/leaf id
func (t *PBSTree) genrateID() int64 {
	var id int64
	id = -1
	for k := range t.dupnodelist {
		id = k
		delete(t.dupnodelist, k)
		break
	}
	if id == -1 {
		if t.GetIndexCursor() >= t.GetTSize() {
			t.Nodes = append(t.Nodes, make([]*Node, TreeSize)...)
			t.TSize += int64(TreeSize)
		}
		id = t.GetIndexCursor()
		t.IndexCursor++
	}
	return id
}

func (t *PBSTree) newNode(nodeType Node_Type) *Node {
	return &Node{
		Id:     t.genrateID(),
		IsDirt: 0,
		Type:   nodeType,
	}
}

func (t *PBSTree) getNode(id int64) (*Node, error) {

	if n := t.Nodes[id]; n != nil {
		return n, nil
	}

	return nil, ErrNoData
}

func (t *PBSTree) String() string {
	var buf bytes.Buffer

	buf.WriteString("-----------Tree-------------\n")
	buf.WriteString(fmt.Sprintf("Root %d\n", t.GetRoot()))
	buf.WriteString(fmt.Sprintf("IndexCursor %d\n", t.GetIndexCursor()))

	for i := int64(0); i < t.GetIndexCursor(); i++ {
		if node, err := t.getNode(i); err == nil {
			if node.GetIsDirt() == 0 {
				buf.WriteString(node.String())
				buf.WriteString("\t--------\n")
			}
		}
	}

	return buf.String()
}

// Marshal btree to disk
func (t PBSTree) Marshal() ([]byte, error) {

	cut := 0

	for x, y := range t.Nodes {
		if y == nil {
			cut = x
			break
		}
	}

	if cut > 0 {
		t.Nodes = t.Nodes[:cut]
	}

	data, err := proto.Marshal(&t.MetaData)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Unmarshal btree from disk
func (t *PBSTree) Unmarshal(data []byte) error {

	tree := &PBSTree{
		dupnodelist: make(map[int64]int),
	}

	tree.MetaData = MetaData{}

	return proto.Unmarshal(data, &tree.MetaData)
}
