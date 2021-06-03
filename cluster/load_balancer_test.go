package cluster_test

import (
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestRoundRobinLoadBalancer_OneOf(t *testing.T) {
	var lb cluster.LoadBalancer = cluster.NewRoundRobinLoadBalancer()
	targetAddrs := []cluster.Address{"a:5071", "b:5701", "c:5701", "a:5071", "b:5701"}
	var addrs []cluster.Address
	for i := 0; i < 5; i++ {
		addrs = append(addrs, lb.OneOf([]cluster.Address{"a:5071", "b:5701", "c:5701"}))
	}
	assert.Equal(t, targetAddrs, addrs)
}

func TestRoundRobinLoadBalancer_OneOf_ConnectionLost(t *testing.T) {
	lb := cluster.NewRoundRobinLoadBalancer()
	// directly set the index
	(*lb) = 5
	addr := lb.OneOf([]cluster.Address{"a:5071", "b:5701", "c:5701"})
	// since the index is greater than available addresses, LB should return the last address
	assert.Equal(t, cluster.Address("c:5701"), addr)
}

func TestRandomLoadBalancer_OneOf(t *testing.T) {
	var lb cluster.LoadBalancer
	// seed the random load balancer with a fixed int
	r := rand.New(rand.NewSource(5))
	rlb := cluster.RandomLoadBalancer(*r)
	lb = &rlb
	targetAddrs := []cluster.Address{"a:5071", "b:5701", "b:5701", "b:5701", "a:5071"}
	var addrs []cluster.Address
	for i := 0; i < 5; i++ {
		addrs = append(addrs, lb.OneOf([]cluster.Address{"a:5071", "b:5701", "c:5701"}))
	}
	assert.Equal(t, targetAddrs, addrs)
}
