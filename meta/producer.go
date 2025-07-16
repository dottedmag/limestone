package meta

import (
	"maps"
	"slices"
	"strings"
)

// Producer is a generic service name
type Producer string

// ProducerSet is a set of allowed Producer values
type ProducerSet map[Producer]bool

// String implements fmt.Stringer
func (ps ProducerSet) String() string {
	var res string
	for _, p := range slices.Sorted(maps.Keys(ps)) {
		if res != "" {
			res += "|"
		}
		res += string(p)
	}
	return res
}

// ParseProducerSet parses a string into a ProducerSet. The string is
// a list of alternatives separated by "|". Other characters are matched
// exactly.
func ParseProducerSet(pattern string) ProducerSet {
	res := ProducerSet{}
	for _, p := range strings.Split(pattern, "|") {
		if p == "" {
			panicf("invalid producer rule %q", pattern)
		}
		if res[Producer(p)] {
			panicf("duplicate producer %q in pattern %q", p, pattern)
		}
		res[Producer(p)] = true
	}
	return res
}
