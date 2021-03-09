package cluster

import (
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"sync/atomic"
)

type Service interface {
	GetMemberByUUID(uuid string) hazelcast.Member
	Members() []hazelcast.Member
	OwnerConnectionAddr() *hazelcast.Address
}

type ServiceImpl struct {
	ownerConnectionAddr atomic.Value
	addrProviders       []AddressProvider
	ownerUUID           atomic.Value
	uuid                atomic.Value
}

func NewServiceImpl(addrProviders []AddressProvider) *ServiceImpl {
	service := &ServiceImpl{addrProviders: addrProviders}
	service.ownerConnectionAddr.Store(&hazelcast.Address{})
	return service
}

func (s *ServiceImpl) GetMemberByUUID(uuid string) hazelcast.Member {
	panic("implement me")
}

func (s *ServiceImpl) Members() []hazelcast.Member {
	panic("implement me")
}

func (s *ServiceImpl) OwnerConnectionAddr() *hazelcast.Address {
	if addr, ok := s.ownerConnectionAddr.Load().(*hazelcast.Address); ok {
		return addr
	}
	return nil
}

func (s *ServiceImpl) memberCandidateAddrs() []*hazelcast.Address {
	addrSet := NewAddrSet()
	for _, addrProvider := range s.addrProviders {
		addrSet.AddAddrs(addrProvider.Addresses())
	}
	return addrSet.Addrs()
}

type AddrSet struct {
	addrs map[string]*hazelcast.Address
}

func NewAddrSet() AddrSet {
	return AddrSet{addrs: map[string]*hazelcast.Address{}}
}

func (a AddrSet) AddAddr(addr *hazelcast.Address) {
	a.addrs[addr.String()] = addr
}

func (a AddrSet) AddAddrs(addrs []*hazelcast.Address) {
	for _, addr := range addrs {
		a.AddAddr(addr)
	}
}

func (a AddrSet) Addrs() []*hazelcast.Address {
	addrs := make([]*hazelcast.Address, 0, len(a.addrs))
	for _, addr := range a.addrs {
		addrs = append(addrs, addr)
	}
	return addrs
}
