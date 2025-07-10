package kafka

import (
	"github.com/dottedmag/limestone/kafka/api"
	"github.com/dottedmag/limestone/kafka/isempty"
	"github.com/dottedmag/limestone/kafka/names"
	"github.com/dottedmag/limestone/kafka/uri"
)

// Client is the Kafka client interface
type Client = api.Client

// ClientBackdate is an extended interface implemented by some clients
type ClientBackdate = api.ClientBackdate

// Message is an outgoing Kafka message
type Message = api.Message

// IncomingMessage is an incoming Kafka message
type IncomingMessage = api.IncomingMessage

// ValidateTopicName returns an error if the given topic name is invalid
var ValidateTopicName = names.ValidateTopicName

// TopicIsEmpty finds out whether a Kafka topic is empty
var TopicIsEmpty = isempty.TopicIsEmpty

// FromURI creates a client from an URI
var FromURI = uri.FromURI
