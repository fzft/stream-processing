package stream_processing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type TraverserTest struct {
	t *AppendableTraverser
}

func TraverserTestSetup(tb testing.TB) (func(tb testing.TB), TraverserTest) {
	tt := TraverserTest{}
	tt.t = NewAppendableTraverser()

	return func(tb testing.TB) {
		tb.Log("TraverserTestSetup teardown")
	}, tt
}

func TestAppendableTraverser_smoke(t *testing.T) {
	teardownTest, tt := TraverserTestSetup(t)
	defer teardownTest(t)

	assert.True(t, tt.t.isEmpty())
	tt.t.append("1")
	assert.False(t, tt.t.isEmpty())
	assert.Equal(t, "1", tt.t.next())
	assert.True(t, tt.t.isEmpty())
	assert.Nil(t, tt.t.next())

	tt.t.append("2")
	tt.t.append("3")
	assert.Equal(t, "2", tt.t.next())
	assert.Equal(t, "3", tt.t.next())
	assert.Nil(t, tt.t.next())
}

