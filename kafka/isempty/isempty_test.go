package isempty

import (
	"testing"

	"github.com/dottedmag/limestone/kafka/api"
	"github.com/dottedmag/limestone/kafka/mock"
	"github.com/dottedmag/limestone/test"
	"github.com/stretchr/testify/require"
)

func TestTopicIsEmpty(t *testing.T) {
	ctx := test.Context(t)

	k := mock.New()

	empty, err := TopicIsEmpty(ctx, k, "foo")
	require.NoError(t, err)
	require.True(t, empty)

	require.NoError(t, k.Write(ctx, "foo", []api.Message{{
		Topic: "foo",
		Value: []byte("FOO"),
	}}))

	empty, err = TopicIsEmpty(ctx, k, "foo")
	require.NoError(t, err)
	require.False(t, empty)
}
