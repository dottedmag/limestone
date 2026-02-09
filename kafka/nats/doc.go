// Package nats is a NATS JetStream implementation of kafka.Client. Each
// Limestone topic maps to a JetStream stream with a single subject.
//
// Unlike Kafka, NATS JetStream uses 1-based sequence numbers. This package
// translates between Limestone's 0-based offsets and JetStream's 1-based
// sequences internally.
package nats
