package internal

import (
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ClientConnectionManager interface {
	IsAlive() bool

	GetConnection(uuid core.UUID) TcpClientConnection

	CheckIfInvocationAllowed() error

	GetClientUUID() core.UUID

	GetRandomConnection() TcpClientConnection

	GetActiveConnections() []TcpClientConnection

	AddConnectionListener(listener connectionListener)

	TryConnectToAllClusterMembers()
}

type clientConnectionManager struct {
	client                  *HazelcastClient
	connectionListeners     []connectionListener
	addressTranslator       AddressTranslator
	connectionListenersMU   sync.RWMutex
	loadBalancer            core.LoadBalancer
	activeConnections       sync.Map
	labels                  []string
	logger                  logger.Logger
	connectionTimeoutMillis time.Duration
	heartbeatManager        *heartBeatService
	authenticationTimeout   time.Duration
	clientUUID              core.UUID
	shuffleMemberList       bool
	smartRoutingEnabled     bool
	asyncStart              bool
	isAlive                 atomic.Value
}

func newClientConnectionManager(client *HazelcastClient, addressTranslator AddressTranslator) ClientConnectionManager {
	isAlive := atomic.Value{}
	isAlive.Store(false)

	return &clientConnectionManager{
		client:                  client,
		connectionListeners:     make([]connectionListener, 0),
		addressTranslator:       addressTranslator,
		loadBalancer:            client.Config.LoadBalancer(),
		labels:                  client.Config.GetLabels(),
		logger:                  client.logger,
		connectionTimeoutMillis: initConnectionTimeoutMillis(client),
		heartbeatManager:        client.HeartBeatService,
		authenticationTimeout:   client.HeartBeatService.heartBeatTimeout,
		clientUUID:              core.NewUUID(),
		shuffleMemberList:       client.properties.GetBoolean(property.StatisticsEnabled),
		smartRoutingEnabled:     client.Config.NetworkConfig().IsSmartRouting(),
		isAlive:                 isAlive,
	}
}

func initConnectionTimeoutMillis(client *HazelcastClient) time.Duration {
	networkConfig := client.Config.NetworkConfig()
	connTimeout := networkConfig.ConnectionTimeout()
	if connTimeout == 0 {
		return math.MaxInt32 * time.Millisecond
	}
	return connTimeout
}

func (c *clientConnectionManager) Start() {
	if !c.IsAlive() {
		return
	}
	c.heartbeatManager.start()
	c.connectToCluster()
}

func (c *clientConnectionManager) TryConnectToAllClusterMembers() {
	if !c.smartRoutingEnabled {
		return
	}

	for _, eachMember := range c.client.getClientClusterService().GetMemberList() {
		c.getOrConnectToMember(eachMember)
	}

	//TODO ConnectionManagementTask should add and it will work every 1 sec.
}

func (c *clientConnectionManager) connectToCluster() {
	if c.asyncStart {
		c.submitConnectToClusterTask()
	} else {
		c.doConnectToCluster()
	}
}

func (c *clientConnectionManager) submitConnectToClusterTask() {
	panic("implement me")
}

func (c *clientConnectionManager) doConnectToCluster() {
	c.client.ClusterService.connectToCluster()
}

func (c *clientConnectionManager) connectToAllClusterMembers() {
	for _, eachMember := range c.client.ClusterService.GetMemberList() {
		c.getOrConnectToMember(eachMember)
	}
}

func (c *clientConnectionManager) getOrConnectToMember(member core.Member) TcpClientConnection {
	uuid := member.UUID()
	connectionLoad, connectionLoadOk := c.activeConnections.Load(uuid.ToString())
	if connectionLoadOk {
		return connectionLoad.(TcpClientConnection)
	}

	address := member.Address()
	address = c.addressTranslator.Translate(address)
	c.createSocketConnection(address)
	return nil
}

func (c *clientConnectionManager) IsAlive() bool {
	return c.isAlive.Load().(bool)
}

func (c *clientConnectionManager) GetConnection(uuid core.UUID) TcpClientConnection {
	connection, _ := c.activeConnections.Load(uuid.ToString())
	return connection.(TcpClientConnection)
}

func (c *clientConnectionManager) CheckIfInvocationAllowed() error {
	panic("implement me")
}

func (c *clientConnectionManager) GetClientUUID() core.UUID {
	return c.clientUUID
}

func (c *clientConnectionManager) GetRandomConnection() TcpClientConnection {
	if c.smartRoutingEnabled {
		member := c.loadBalancer.Next()
		if member != nil {
			return c.GetConnection(member.UUID())
		}
	}

	for _, connection := range c.GetActiveConnections() {
		return connection
	}

	return nil
}

func (c *clientConnectionManager) GetActiveConnections() []TcpClientConnection {
	connections := make([]TcpClientConnection, 0)
	c.activeConnections.Range(func(key, value interface{}) bool {
		connections = append(connections, value.(TcpClientConnection))
		return true
	})
	return connections
}

func (c *clientConnectionManager) AddConnectionListener(listener connectionListener) {
	c.connectionListenersMU.Lock()
	defer c.connectionListenersMU.Unlock()
	c.connectionListeners = append(c.connectionListeners, listener)
}

func (c *clientConnectionManager) createSocketConnection(address core.Address) TcpClientConnection {
	conn, err := net.DialTimeout("tcp", address.String(), c.connectionTimeoutMillis)
	if err != nil {
		return nil
	}
	connection := NewTcpClientConnection(c.client, 100, conn)
	status, address, memberUuid, serializationVersion, serverHazelcastVersion, partitionCount, clusterId, failoverSupported, err := c.authenticateOnCluster(connection, address)
	onAuthenticated(connection, status, address, memberUuid, serializationVersion, serverHazelcastVersion, partitionCount, clusterId, failoverSupported)
	return connection
}

func (c *clientConnectionManager) authenticateOnCluster(connection TcpClientConnection, address core.Address) (byte, core.Address, core.UUID, byte, string, int32, core.UUID, bool, error) {
	authenticationRequest := c.encodeAuthenticationRequest(address)
	newClientInvocation := NewClientInvocation(c.client, authenticationRequest, connection)
	clientMessage, _ := newClientInvocation.Inkove()

	status, address, memberUuid, serializationVersion, serverHazelcastVersion, partitionCount, clusterId, failoverSupported := codec.ClientAuthenticationCodec.DecodeResponse(clientMessage)

	switch status {
	case authenticated:
		return status, address, memberUuid, serializationVersion, serverHazelcastVersion, partitionCount, clusterId, failoverSupported, nil
	}

	return 0, nil, nil, 0, "", 0, nil, false, core.NewHazelcastAuthenticationError("Authentication status code not supported.", nil)
}

func onAuthenticated(connection TcpClientConnection, status byte, address core.Address, uuid core.UUID, version byte, version2 string, count int32, id core.UUID, supported bool) {

}

func (c *clientConnectionManager) encodeAuthenticationRequest(address core.Address) *proto.ClientMessage {
	return codec.ClientAuthenticationCodec.EncodeRequest(
		"dev",
		"",
		"",
		core.NewUUID(),
		proto.ClientType,
		byte(serializationVersion),
		ClientVersion,
		c.client.name,
		c.labels,
	)
}
