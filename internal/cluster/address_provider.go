package cluster

import (
    "github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
    "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/config"
) 

type AddressProvider interface {
	Addresses() []*AddressImpl
}

type DefaultAddressProvider struct {
	addresses []*AddressImpl
}

func NewDefaultAddressProvider(networkConfig *config.NetworkConfig) *DefaultAddressProvider {
	var err error
	addresses := make([]*AddressImpl, len(networkConfig.Addresses()))
	for i, addr := range networkConfig.Addresses() {
		if addresses[i], err = hazelcast.ParseAddress(addr); err != nil {
			panic(err)
		}
	}
	return &DefaultAddressProvider{addresses: addresses}
}

func (p DefaultAddressProvider) Addresses() []*AddressImpl {
	return p.addresses
}
