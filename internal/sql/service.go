package sql

import (
	"fmt"
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/sql"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	icluster "github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

type Service struct {
	serializationService serialization.Service
	connectionManager    *icluster.ConnectionManager
	clusterService       icluster.Service
	requestCh            chan<- invocation.Invocation
}

func NewSqlService(connectionManager *icluster.ConnectionManager, clusterService icluster.Service, requestCh chan<- invocation.Invocation, serializationService serialization.Service) *Service {
	return &Service{connectionManager: connectionManager, clusterService: clusterService, requestCh: requestCh, serializationService: serializationService}
}

func (s *Service) Execute(command string) (sql.Result, error) {
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

	colMetadataList, page, updateCount, err := codec.DecodeSqlExecuteResponse(v)

	var rowMetadata *sql.SqlRowMetadata
	if colMetadataList != nil {
		rowMetadata = sql.NewRowMetadata(colMetadataList)
	} else {
		rowMetadata = nil
	}
	if err.Message() != "" || err.Code() != 0 {
		return nil, &err
	}
	return sql.NewResult(page, rowMetadata, updateCount, s.serializationService), nil
}

func (s *Service) invoke(request *proto.ClientMessage, address pubcluster.Address) (*proto.ClientMessage, error) {
	inv := invocation.NewImpl(request, 0, address, -1)
	s.requestCh <- inv
	return inv.Get()
}
