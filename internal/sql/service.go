package sql

import (
	"fmt"
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proxy"
)

type Service struct {
	proxy proxy.Impl
	connectionManager cluster.ConnectionManager
	clusterService cluster.Service
}

func NewSqlService(connectionManager cluster.ConnectionManager, clusterService cluster.Service) Service {
	return Service{connectionManager: connectionManager, clusterService: clusterService}
}

func (s *Service) Execute(command string) bool {
	fmt.Println("Executing SQL service: ", command)

	membersMap := s.clusterService.GetMembersMap()
	if len(membersMap) == 0 {
		panic("No members")
	}

	var memberId internal.UUID
	var memberAddress pubcluster.Address
	// Get random member
	for _, v := range membersMap {
		memberId = v.UUID()
		memberAddress = v.Address()
		break
	}


	connection := s.connectionManager.GetConnectionForAddress(memberAddress)

	if connection == nil {
		panic("Connection is not connected to cluster")
	}

	localId := internal.NewUUID()
	queryId := NewQueryId(memberId.MostSignificantBits(), memberId.LeastSignificantBits(), localId.MostSignificantBits(), localId.LeastSignificantBits())


	requestMessage := codec.EncodeSqlExecuteRequest(command, nil, -1, 4096, "", 0, queryId)

	fmt.Println(requestMessage)

	go func(){
		s.invoke(requestMessage, memberAddress)
	}()

	return true
}

func (s *Service) invoke(request *proto.ClientMessage, address pubcluster.Address) (*proto.ClientMessage, error) {
	inv := s.proxy.InvocationFactory.NewInvocationOnTarget(request, address)
	s.proxy.RequestCh <- inv
	return inv.Get()
	//select {
	//case p.requestCh <- inv:
	//	return inv.GetWithTimeout(100 * time.Millisecond)
	//case <-time.After(100 * time.Millisecond):
	//	return nil, errors.New("timeout")

	//}

}