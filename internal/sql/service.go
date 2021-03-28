package sql

import (
	"fmt"
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/sql"
	icluster "github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/codec"
)

type Service struct {
	invocationService *invocation.ServiceImpl
	connectionManager *icluster.ConnectionManager
	clusterService icluster.Service
}

func NewSqlService(connectionManager *icluster.ConnectionManager, clusterService icluster.Service, invocationService *invocation.ServiceImpl) *Service {
	return &Service{connectionManager: connectionManager, clusterService: clusterService, invocationService: invocationService}
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
	queryId := sql.NewQueryId(int64(memberId.MostSignificantBits()), int64(memberId.LeastSignificantBits()), int64(localId.MostSignificantBits()), int64(localId.LeastSignificantBits()))


	requestMessage := codec.EncodeSqlExecuteRequest(command, nil, -1, 4096, "", 0, queryId)


	v, _ := s.invoke(requestMessage, memberAddress)

	rowM, page, updateCount, err := codec.DecodeSqlExecuteResponse(v)

	fmt.Print("Rows Metadata:")
	fmt.Println(rowM)
	fmt.Print("Page:")
	fmt.Println(page.ColumnValuesForServer(0))
	fmt.Print("Update c:")
	fmt.Println(updateCount)
	fmt.Print("Error:")
	fmt.Println(err)

	return true
}

func (s *Service) invoke(request *proto.ClientMessage, address pubcluster.Address) (*proto.ClientMessage, error) {
	inv := s.invocationService.SendInvocation(invocation.NewImpl(request, 0, address, -1))
	return inv.Get()
}
