package client

import (
	"context"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/dottedmag/limestone/wire"
	"github.com/dottedmag/parallel"
)

func TestSplitter(t *testing.T) {
	env := kafkaTestSetup(t)
	splitter := NewSplitter(env.client)
	conn1 := splitter.Connect(0, wire.Beginning, nil, false)
	conn2 := splitter.Connect(0, wire.Beginning, nil, false)
	incoming1 := make(chan *wire.IncomingTransaction)
	incoming2 := make(chan *wire.IncomingTransaction)
	env.group.Spawn("conn1", parallel.Fail, func(ctx context.Context) error {
		return conn1.Run(ctx, incoming1)
	})
	env.group.Spawn("conn2", parallel.Fail, func(ctx context.Context) error {
		return conn2.Run(ctx, incoming2)
	})
	env.group.Spawn("splitter", parallel.Fail, splitter.Run)

	assert.NoError(t, PublishKafkaManifest(env.group.Context(), env.kafka, wire.Manifest{Topic: "txlog"}))
	assert.Zero(t, <-incoming1)
	assert.Zero(t, <-incoming2)

	assert.NoError(t, conn1.Submit(env.group.Context(), testTxn1))
	in1 := <-incoming1
	assert.Equal(t, wire.Position("0000000000000000-0000000000000000"), in1.Position)
	assert.NotZero(t, in1.TS)
	assert.Equal(t, testTxn1.Source, in1.Source)
	assert.Equal(t, testTxn1.Session, in1.Session)
	assert.Equal(t, testTxn1.Changes, in1.Changes)
	in2 := <-incoming2
	assert.Equal(t, wire.Position("0000000000000000-0000000000000000"), in2.Position)
	assert.NotZero(t, in2.TS)
	assert.Equal(t, testTxn1.Source, in2.Source)
	assert.Equal(t, testTxn1.Session, in2.Session)
	assert.Equal(t, testTxn1.Changes, in2.Changes)
	assert.Equal(t, in1.TS, in2.TS)
	assert.Zero(t, <-incoming1)
	assert.Zero(t, <-incoming2)

	assert.NoError(t, conn2.Submit(env.group.Context(), testTxn2))
	in1 = <-incoming1
	assert.Equal(t, wire.Position("0000000000000000-0000000000000001"), in1.Position)
	assert.NotZero(t, in1.TS)
	assert.Equal(t, testTxn2.Source, in1.Source)
	assert.Equal(t, testTxn2.Session, in1.Session)
	assert.Equal(t, testTxn2.Changes, in1.Changes)
	in2 = <-incoming2
	assert.Equal(t, wire.Position("0000000000000000-0000000000000001"), in2.Position)
	assert.NotZero(t, in2.TS)
	assert.Equal(t, testTxn2.Source, in2.Source)
	assert.Equal(t, testTxn2.Session, in2.Session)
	assert.Equal(t, testTxn2.Changes, in2.Changes)
	assert.Equal(t, in1.TS, in2.TS)
	assert.Zero(t, <-incoming1)
	assert.Zero(t, <-incoming2)
}

func TestFilterUnion(t *testing.T) {
	f1 := wire.Filter{
		"foo": nil,
		"bar": []string{"a", "b"},
		"baz": []string{"z"},
		"qux": []string{"y"},
	}
	f2 := wire.Filter{
		"foo": []string{"x"},
		"bar": []string{"b", "c"},
		"baz": nil,
		"uvw": []string{"w"},
	}

	var f wire.Filter
	filterUnion(&f, f1)
	assert.Zero(t, f)

	f = f1
	filterUnion(&f, nil)
	assert.Zero(t, f)

	f = f1
	filterUnion(&f, f1)
	assert.Equal(t, f1, f)

	filterUnion(&f, f2)
	assert.Equal(t, wire.Filter{
		"foo": nil,
		"bar": []string{"a", "b", "c"},
		"baz": nil,
		"qux": []string{"y"},
		"uvw": []string{"w"},
	}, f)
}
