package pbst

import (
	"strconv"
	"testing"
	"github.com/stretchr/testify/assert"
)

var data []byte

func TestInsert(t *testing.T) {
	tree := NewPBSTWithSize(2, 2)
	size := 2

	for i := 0; i < size; i++ {
		assert.Nil(t, tree.Insert([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))))
	}
}

func TestSearch(t *testing.T) {

	tree := NewPBSTWithSize(2, 3)
	size := 100

	for i := 0; i < size; i++ {
		assert.Nil(t, tree.Insert([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))))
	}

	for i := 0; i < size; i++ {
		rst, err := tree.Search([]byte(strconv.Itoa(i)))
		assert.Nil(t, err)
		assert.NotNil(t, rst)
		assert.Equal(t, strconv.Itoa(i), string(rst))
	}
}

func TestUpdate(t *testing.T) {

	tree := NewPBSTWithSize(3, 2)
	size := 100

	for i := 0; i < size; i++ {
		assert.Nil(t, tree.Insert([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))))
	}

	for i := 0; i < size; i++ {
		assert.Nil(t, tree.Update([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i+1))))
	}
	for i := 0; i < size; i++ {
		rst, _ := tree.Search([]byte(strconv.Itoa(i)))
		if string(rst) != strconv.Itoa(i+1) {
			t.Fatal("failed", i)
		}
	}
}

func TestDelete(t *testing.T) {

	tree := NewPBSTWithSize(3, 3)
	size := 100

	for i := 0; i < size; i++ {
		assert.Nil(t, tree.Insert([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))))
	}
	for i := 0; i < size; i++ {
		assert.Nil(t, tree.Delete([]byte(strconv.Itoa(i))))
		_, err := tree.Search([]byte(strconv.Itoa(i)))
		assert.NotNil(t, err)
	}
}

func TestSerialize(t *testing.T) {

	size := 100000
	tree := NewPBST()
	for i := 0; i < size; i++ {
		assert.Nil(t, tree.Insert([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))))
	}

	var err error

	data, err = tree.Marshal()
	assert.Nil(t, err)
	assert.NotNil(t, data)

}

func TestSerializeDeserialize(t *testing.T) {

	tree := PBSTree{}

	assert.Nil(t, tree.Unmarshal(data))

	for i := 0; i < int(tree.TSize); i++ {
		rst, err := tree.Search([]byte(strconv.Itoa(i)))
		assert.Nil(t, err)
		assert.NotNil(t, rst)
		assert.Equal(t, strconv.Itoa(i), string(rst))
	}

}


func BenchmarkInsert(b *testing.B) {

	size := 100000
	tree := NewPBST()
	for i := 0; i < size; i++ {
		assert.Nil(b, tree.Insert([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))))
	}
}

func BenchmarkSearch(b *testing.B) {

	size := 100000
	tree := NewPBST()
	for i := 0; i < size; i++ {
		assert.Nil(b, tree.Insert([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))))
	}

	b.ResetTimer()

	for i := 0; i < size; i++ {
		rst, err := tree.Search([]byte(strconv.Itoa(i)))
		assert.Nil(b, err)
		if string(rst) != strconv.Itoa(i) {
			b.Fatal("failed", i)
		}
	}
}

func BenchmarkUpdate(b *testing.B) {

	size := 100000
	tree := NewPBST()
	for i := 0; i < size; i++ {
		assert.Nil(b, tree.Insert([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))))
	}

	b.ResetTimer()

	for i := 0; i < size; i++ {
		if err := tree.Update([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i+1))); err != nil {
			b.Fatal("failed", i, err)
		}
	}

	b.StopTimer()

	for i := 0; i < size; i++ {
		rst, _ := tree.Search([]byte(strconv.Itoa(i)))
		if string(rst) != strconv.Itoa(i+1) {
			b.Fatal("Find Failed", i)
		}
	}
}

func BenchmarkDelete(b *testing.B) {

	size := 100000
	tree := NewPBST()
	for i := 0; i < size; i++ {
		assert.Nil(b, tree.Insert([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))))
	}

	b.ResetTimer()

	for i := 0; i < size; i++ {

		b.StartTimer()

		if err := tree.Delete([]byte(strconv.Itoa(i))); err != nil {
			b.Fatal("failed", i)
		}

		b.StopTimer()

		if _, err := tree.Search([]byte(strconv.Itoa(i))); err == nil {
			b.Fatal("Find Failed", i)
		}
	}
}
