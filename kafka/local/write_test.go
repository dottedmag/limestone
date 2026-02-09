package local

import (
	"os"
	"testing"

	"github.com/dottedmag/limestone/kafka/api"
	"github.com/dottedmag/must"
	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {
	env := setupTest(t)

	require.NoError(t, env.client.WriteBackdated(env.group.Context(), "foo", []api.IncomingMessage{
		{
			Message: api.Message{Topic: "foo", Key: "666", Value: []byte("FOO-666")},
			Time:    testTime,
		},
		{
			Message: api.Message{Topic: "foo", Key: "777", Value: []byte("FOO-777")},
			Time:    testTime,
		},
	}))
	require.NoError(t, env.client.WriteBackdated(env.group.Context(), "bar", []api.IncomingMessage{
		{
			Message: api.Message{Topic: "bar", Headers: map[string]string{"a": "b", "c": "d"}, Value: []byte("BAR")},
			Time:    testTime,
		},
	}))

	require.Equal(t, lines(
		`{"TS":"2020-01-01T12:00:00Z","Key":"666","Len":7}`,
		`FOO-666`,
		`{"TS":"2020-01-01T12:00:00Z","Key":"777","Len":7}`,
		`FOO-777`,
	), string(must.OK1(os.ReadFile(env.dir+"/foo"))))
	require.Equal(t, lines(
		`{"TS":"2020-01-01T12:00:00Z","Headers":{"a":"b","c":"d"},"Len":3}`,
		`BAR`,
	), string(must.OK1(os.ReadFile(env.dir+"/bar"))))

	require.NoError(t, env.client.WriteBackdated(env.group.Context(), "bar", []api.IncomingMessage{
		{
			Message: api.Message{Topic: "bar", Value: []byte{}},
			Time:    testTime,
		},
	}))
	require.Equal(t, lines(
		`{"TS":"2020-01-01T12:00:00Z","Headers":{"a":"b","c":"d"},"Len":3}`,
		`BAR`,
		`{"TS":"2020-01-01T12:00:00Z","Len":0}`,
		``,
	), string(must.OK1(os.ReadFile(env.dir+"/bar"))))
}
