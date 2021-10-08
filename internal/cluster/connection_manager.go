/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/security"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/util"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	authenticated                = 0
	credentialsFailed            = 1
	serializationVersionMismatch = 2
	notAllowedInCluster          = 3
)

const (
	created int32 = iota
	ready
	stopped
	restarting
)

const (
	serializationVersion  = 1
	initialMembersTimeout = 120 * time.Second
)

const (
	disconnected = 0
	connected    = 1
)

type connectMemberFunc func(ctx context.Context, m *ConnectionManager, addr pubcluster.Address, member *pubcluster.MemberInfo) (pubcluster.Address, error)

type ConnectionManagerCreationBundle struct {
	Logger               ilogger.Logger
	PartitionService     *PartitionService
	InvocationFactory    *ConnectionInvocationFactory
	ClusterConfig        *pubcluster.Config
	ClusterService       *Service
	SerializationService *iserialization.Service
	EventDispatcher      *event.DispatchService
	FailoverService      *FailoverService
	FailoverConfig       *pubcluster.FailoverConfig
	ClientName           string
	Labels               []string
}

func (b ConnectionManagerCreationBundle) Check() {
	if b.Logger == nil {
		panic("Logger is nil")
	}
	if b.ClusterService == nil {
		panic("ClusterService is nil")
	}
	if b.PartitionService == nil {
		panic("PartitionService is nil")
	}
	if b.SerializationService == nil {
		panic("SerializationService is nil")
	}
	if b.EventDispatcher == nil {
		panic("EventDispatcher is nil")
	}
	if b.InvocationFactory == nil {
		panic("InvocationFactory is nil")
	}
	if b.ClusterConfig == nil {
		panic("ClusterConfig is nil")
	}
	if b.ClientName == "" {
		panic("ClientName is blank")
	}
	if b.FailoverService == nil {
		panic("FailoverService is nil")
	}
	if b.FailoverConfig == nil {
		panic("FailoverConfig is nil")
	}
}

type ConnectionManager struct {
	logger               ilogger.Logger
	failoverConfig       *pubcluster.FailoverConfig
	partitionService     *PartitionService
	serializationService *iserialization.Service
	eventDispatcher      *event.DispatchService
	invocationFactory    *ConnectionInvocationFactory
	clusterService       *Service
	invocationService    *invocation.Service
	startCh              chan struct{}
	connMap              *connectionMap
	doneCh               chan struct{}
	clusterConfig        *pubcluster.Config
	clusterIDMu          *sync.Mutex
	clusterID            types.UUID
	prevClusterID        types.UUID
	failoverService      *FailoverService
	randGen              *rand.Rand
	clientName           string
	labels               []string
	clientUUID           types.UUID
	nextConnID           int64
	state                int32
	connectionState      int32
	smartRouting         bool
	memberAddCh          chan types.UUID
	memberRemoveCh       chan *Connection
}

func NewConnectionManager(bundle ConnectionManagerCreationBundle) *ConnectionManager {
	bundle.Check()
	manager := &ConnectionManager{
		clusterService:       bundle.ClusterService,
		partitionService:     bundle.PartitionService,
		serializationService: bundle.SerializationService,
		eventDispatcher:      bundle.EventDispatcher,
		invocationFactory:    bundle.InvocationFactory,
		clusterConfig:        bundle.ClusterConfig,
		clientName:           bundle.ClientName,
		labels:               bundle.Labels,
		clientUUID:           types.NewUUID(),
		connMap:              newConnectionMap(bundle.ClusterConfig.LoadBalancer()),
		smartRouting:         !bundle.ClusterConfig.Unisocket,
		logger:               bundle.Logger,
		failoverService:      bundle.FailoverService,
		failoverConfig:       bundle.FailoverConfig,
		clusterIDMu:          &sync.Mutex{},
		randGen:              rand.New(rand.NewSource(time.Now().Unix())),
		memberAddCh:          make(chan types.UUID, 1024),
		memberRemoveCh:       make(chan *Connection, 1024),
		doneCh:               make(chan struct{}),
		startCh:              make(chan struct{}),
	}
	return manager
}

func (m *ConnectionManager) Start(ctx context.Context) error {
	addr, err := m.start(ctx)
	if err != nil {
		return err
	}
	if m.smartRouting {
		go m.syncConnections()
	}
	m.eventDispatcher.Subscribe(EventConnectionClosed, event.DefaultSubscriptionID, m.handleConnectionClosed)
	atomic.StoreInt32(&m.state, ready)
	m.eventDispatcher.Publish(NewConnected(addr))
	return nil
}

func (m *ConnectionManager) Restart(ctx context.Context) error {
	//if !atomic.CompareAndSwapInt32(&m.state, ready, restarting) {
	//	return nil
	//}
	m.logger.Trace(func() string {
		return "cluster.ConnectionManager.Restart"
	})
	m.startCh = make(chan struct{})
	m.connMap.Reset()
	m.clusterIDMu.Lock()
	if !m.clusterID.Default() {
		m.prevClusterID = m.clusterID
	}
	// default UUID
	m.clusterID = types.UUID{}
	m.clusterIDMu.Unlock()
	addr, err := m.start(ctx)
	if err != nil {
		return fmt.Errorf("restarting: %w", err)
	}
	// check if cluster ID has changed after reconnection
	var clusterChanged bool
	m.clusterIDMu.Lock()
	clusterChanged = !m.prevClusterID.Default() && !m.prevClusterID.Equal(m.clusterID)
	m.clusterIDMu.Unlock()
	if clusterChanged {
		m.logger.Debug(func() string {
			return fmt.Sprintf("cluster changed from: %s to %s", m.prevClusterID, m.clusterID)
		})
		m.eventDispatcher.Publish(NewChangedCluster())
	}
	//atomic.CompareAndSwapInt32(&m.state, restarting, ready)
	m.eventDispatcher.Publish(NewConnected(addr))
	return nil
}

// SetInvocationService sets the invocation service for the connection manager.
// This method should be called before Start.
func (m *ConnectionManager) SetInvocationService(s *invocation.Service) {
	m.invocationService = s
}

func (m *ConnectionManager) Stop() {
	m.eventDispatcher.Unsubscribe(EventConnectionClosed, event.MakeSubscriptionID(m.handleConnectionClosed))
	m.eventDispatcher.Unsubscribe(EventMembersAdded, event.MakeSubscriptionID(m.handleMembersAdded))
	if !atomic.CompareAndSwapInt32(&m.state, ready, stopped) {
		return
	}
	close(m.doneCh)
	m.connMap.CloseAll(nil)
}

func (m *ConnectionManager) start(ctx context.Context) (pubcluster.Address, error) {
	m.logger.Trace(func() string { return "cluster.ConnectionManager.start" })
	//m.eventDispatcher.Subscribe(EventMembersAdded, event.DefaultSubscriptionID, m.handleInitialMembersAdded)
	m.eventDispatcher.Subscribe(EventMembersAdded, event.DefaultSubscriptionID, m.handleMembersAdded)
	addr, err := m.tryConnectCluster(ctx)
	if err != nil {
		return "", err
	}
	// wait for the initial member list
	m.logger.Debug(func() string { return "cluster.ConnectionManager.start: waiting for the initial member list" })
	select {
	case <-ctx.Done():
		return "", fmt.Errorf("getting initial member list from cluster: %w", ctx.Err())
	case <-m.startCh:
		break
	case <-time.After(initialMembersTimeout):
		return "", fmt.Errorf("timed out getting initial member list from cluster: %w", hzerrors.ErrIllegalState)
	}
	m.logger.Debug(func() string { return "cluster.ConnectionManager.start: received the initial member list" })
	atomic.StoreInt32(&m.connectionState, connected)
	return addr, nil
}

func (m *ConnectionManager) NextConnectionID() int64 {
	return atomic.AddInt64(&m.nextConnID, 1)
}

func (m *ConnectionManager) GetConnectionForAddress(addr pubcluster.Address) *Connection {
	return m.connMap.GetConnectionForAddr(addr)
}

func (m *ConnectionManager) GetConnectionForPartition(partitionID int32) *Connection {
	if partitionID < 0 {
		panic("partition ID is negative")
	}
	uuid, ok := m.partitionService.GetPartitionOwner(partitionID)
	if !ok {
		return nil
	}
	member := m.clusterService.GetMemberByUUID(uuid)
	if member == nil {
		return nil
	}
	return m.GetConnectionForAddress(member.Address)
}

func (m *ConnectionManager) ActiveConnections() []*Connection {
	return m.connMap.Connections()
}

func (m *ConnectionManager) RandomConnection() *Connection {
	return m.connMap.RandomConn()
}

func (m *ConnectionManager) TryConnectCluster(ctx context.Context) (pubcluster.Address, error) {
	return m.tryConnectCluster(ctx)
}

/*
func (m *ConnectionManager) handleInitialMembersAdded(e event.Event) {
	if atomic.LoadInt32(&m.connectionState) != disconnected {
		return
	}
	m.logger.Trace(func() string {
		return "cluster.ConnectionManager.handleInitialMembersAdded"
	})
	m.eventDispatcher.Subscribe(EventMembersAdded, event.DefaultSubscriptionID, m.handleMembersAdded)
	m.eventDispatcher.Unsubscribe(EventMembersAdded, event.MakeSubscriptionID(m.handleInitialMembersAdded))
	m.handleMembersAdded(e)
	close(m.startCh)
}
*/

func (m *ConnectionManager) handleMembersAdded(event event.Event) {
	// do not add new members in non-smart mode
	if !m.smartRouting && m.connMap.Len() > 0 {
		return
	}
	e := event.(*MembersAdded)
	m.logger.Trace(func() string {
		return fmt.Sprintf("cluster.ConnectionManager.handleMembersAdded: %v", e.Members)
	})
	for _, member := range e.Members {
		m.memberAddCh <- member.UUID
	}
	if atomic.LoadInt32(&m.connectionState) == disconnected {
		m.startCh <- struct{}{}
	}
}

func (m *ConnectionManager) handleConnectionClosed(event event.Event) {
	if atomic.LoadInt32(&m.state) != ready {
		return
	}
	if e, ok := event.(*ConnectionClosed); ok {
		m.memberRemoveCh <- e.Conn
	}
}

func (m *ConnectionManager) removeConnection(conn *Connection) {
	remaining := m.connMap.RemoveConnection(conn)
	if remaining > 0 {
		return
	}
	if atomic.CompareAndSwapInt32(&m.connectionState, connected, disconnected) {
		m.eventDispatcher.Publish(NewDisconnected())
	}
}

func (m *ConnectionManager) tryConnectCluster(ctx context.Context) (pubcluster.Address, error) {
	tryCount := 1
	if m.failoverConfig.Enabled {
		tryCount = m.failoverConfig.TryCount
	}
	for i := 1; i <= tryCount; i++ {
		cluster := m.failoverService.Current()
		m.logger.Infof("trying to connect to cluster: %s", cluster.ClusterName)
		addr, err := m.tryConnectCandidateCluster(ctx, cluster, cluster.ConnectionStrategy)
		if err == nil {
			m.logger.Infof("connected to cluster: %s", m.failoverService.Current().ClusterName)
			return addr, nil
		}
		if nonRetryableConnectionErr(err) {
			break
		}
		m.failoverService.Next()
	}
	return "", fmt.Errorf("cannot connect to any cluster: %w", hzerrors.ErrIllegalState)
}

func (m *ConnectionManager) tryConnectCandidateCluster(ctx context.Context, cluster *CandidateCluster, cs *pubcluster.ConnectionStrategyConfig) (pubcluster.Address, error) {
	cbr := cb.NewCircuitBreaker(
		cb.MaxRetries(math.MaxInt32),
		cb.Timeout(time.Duration(cs.Timeout)),
		cb.MaxFailureCount(3),
		cb.RetryPolicy(makeRetryPolicy(m.randGen, &cs.Retry)),
	)
	addr, err := cbr.TryContext(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		addr, err := m.connectCluster(ctx, cluster)
		if err != nil {
			m.logger.Debug(func() string {
				return fmt.Sprintf("cluster.ConnectionManager: error connecting to cluster, attempt %d: %s", attempt+1, err.Error())
			})
		}
		return addr, err
	})
	if err != nil {
		return "", err
	}
	return addr.(pubcluster.Address), nil
}

func (m *ConnectionManager) connectCluster(ctx context.Context, cluster *CandidateCluster) (pubcluster.Address, error) {
	seedAddrs, err := m.clusterService.RefreshedSeedAddrs(cluster)
	if err != nil {
		return "", fmt.Errorf("failed to refresh seed addresses: %w", err)
	}
	if len(seedAddrs) == 0 {
		return "", errors.New("could not find any seed addresses")
	}
	var initialAddr pubcluster.Address
	for _, addr := range seedAddrs {
		initialAddr, err = tryConnectAddress(ctx, m, m.clusterConfig.Network.PortRange, addr, connectMember)
		if err != nil {
			return "", err
		}
	}
	if initialAddr == "" {
		return "", err
	}
	return initialAddr, nil
}

func (m *ConnectionManager) ensureConnection(ctx context.Context, addr pubcluster.Address, member *pubcluster.MemberInfo) (*Connection, error) {
	if conn := m.getConnection(addr); conn != nil {
		return conn, nil
	}
	conn, err := m.maybeCreateConnection(ctx, addr)
	if err != nil {
		return nil, err
	}
	if member != nil {
		conn.memberUUID = member.UUID
	}
	return conn, nil
}

func (m *ConnectionManager) getConnection(addr pubcluster.Address) *Connection {
	return m.GetConnectionForAddress(addr)
}

func (m *ConnectionManager) maybeCreateConnection(ctx context.Context, addr pubcluster.Address) (*Connection, error) {
	// TODO: check whether we can create a connection
	conn := m.createDefaultConnection(addr)
	if err := conn.start(m.clusterConfig, addr); err != nil {
		return nil, ihzerrors.NewTargetDisconnectedError(err.Error(), err)
	} else if err = m.authenticate(ctx, conn); err != nil {
		conn.close(nil)
		return nil, err
	}
	return conn, nil
}

func (m *ConnectionManager) createDefaultConnection(addr pubcluster.Address) *Connection {
	conn := &Connection{
		invocationService: m.invocationService,
		pending:           make(chan invocation.Invocation, 1024),
		doneCh:            make(chan struct{}),
		connectionID:      m.NextConnectionID(),
		eventDispatcher:   m.eventDispatcher,
		status:            0,
		logger:            m.logger,
		clusterConfig:     m.clusterConfig,
	}
	conn.endpoint.Store(addr)
	return conn
}

func (m *ConnectionManager) authenticate(ctx context.Context, conn *Connection) error {
	cluster := m.failoverService.Current()
	m.logger.Debug(func() string {
		return fmt.Sprintf("authenticate: cluster name: %s; local: %s; remote: %s; addr: %s",
			cluster.ClusterName, conn.socket.LocalAddr(), conn.socket.RemoteAddr(), conn.Endpoint())
	})
	credentials := cluster.Credentials
	credentials.SetEndpoint(conn.LocalAddr())
	request := m.encodeAuthenticationRequest()
	inv := m.invocationFactory.NewConnectionBoundInvocation(request, conn, nil, time.Now())
	m.logger.Debug(func() string {
		return fmt.Sprintf("authentication correlation ID: %d", inv.Request().CorrelationID())
	})
	if err := m.invocationService.SendRequest(ctx, inv); err != nil {
		return fmt.Errorf("authenticating: %w", err)
	}
	result, err := inv.GetWithContext(ctx)
	if err != nil {
		return err
	}
	return m.processAuthenticationResult(conn, result)
}

func (m *ConnectionManager) processAuthenticationResult(conn *Connection, result *proto.ClientMessage) error {
	// TODO: use memberUUID v
	status, address, _, _, serverHazelcastVersion, partitionCount, newClusterID, failoverSupported := codec.DecodeClientAuthenticationResponse(result)
	if m.failoverConfig.Enabled && !failoverSupported {
		m.logger.Warnf("cluster does not support failover: this feature is available in Hazelcast Enterprise")
		status = notAllowedInCluster
	}
	switch status {
	case authenticated:
		conn.setConnectedServerVersion(serverHazelcastVersion)
		connAddr, err := m.failoverService.Current().AddressTranslator.Translate(context.TODO(), *address)
		if err != nil {
			return err
		}
		if err := m.partitionService.checkAndSetPartitionCount(partitionCount); err != nil {
			return err
		}

		m.logger.Trace(func() string {
			return fmt.Sprintf("cluster.ConnectionManager: checking the cluster: %v, current cluster: %v", newClusterID, m.clusterID)
		})
		m.clusterIDMu.Lock()
		// clusterID is nil only at the start of the client,
		// or at the start of a reconnection attempt.
		// It is only set in this method below under failoverMu.
		// clusterID is set by master when a cluster is started.
		// clusterID is not preserved during HotRestart.
		// In split brain, both sides have the same clusterID
		clusterIDChanged := !m.clusterID.Default() && !m.clusterID.Equal(newClusterID)
		if clusterIDChanged {
			// If the cluster ID has changed that means we have a connection to wrong cluster.
			// It could also mean a cluster restart.
			// We should not stay connected to this new connection, so we disconnect.
			// In the restart scenario, we just force the disconnect event handler to trigger a reconnection.
			conn.close(nil)
			m.clusterIDMu.Unlock()
			return fmt.Errorf("connection does not belong to this cluster: %w", hzerrors.ErrIllegalState)
		}
		if m.connMap.IsEmpty() {
			// the first connection that opens a connection to the new cluster should set clusterID
			m.clusterID = newClusterID
		}
		m.connMap.AddConnection(conn, connAddr)
		m.clusterIDMu.Unlock()
		m.logger.Debug(func() string {
			return fmt.Sprintf("opened connection to: %s", connAddr)
		})
		m.eventDispatcher.Publish(NewConnectionOpened(conn))
		return nil
	case credentialsFailed:
		return cb.WrapNonRetryableError(fmt.Errorf("invalid credentials: %w", hzerrors.ErrAuthentication))
	case serializationVersionMismatch:
		return cb.WrapNonRetryableError(fmt.Errorf("serialization version mismatches with the server: %w", hzerrors.ErrAuthentication))
	case notAllowedInCluster:
		return cb.WrapNonRetryableError(hzerrors.ErrClientNotAllowedInCluster)
	}
	return hzerrors.ErrAuthentication
}

func (m *ConnectionManager) encodeAuthenticationRequest() *proto.ClientMessage {
	clusterName := m.failoverService.Current().ClusterName
	credentials := m.failoverService.Current().Credentials
	if creds, ok := credentials.(*security.UsernamePasswordCredentials); ok {
		return m.createAuthenticationRequest(clusterName, creds)
	}
	panic("only username password credentials are supported")
}

func (m *ConnectionManager) createAuthenticationRequest(clusterName string, creds *security.UsernamePasswordCredentials) *proto.ClientMessage {
	return codec.EncodeClientAuthenticationRequest(
		clusterName,
		creds.Username(),
		creds.Password(),
		m.clientUUID,
		internal.ClientType,
		byte(serializationVersion),
		internal.ClientVersion,
		m.clientName,
		m.labels,
	)
}

func (m *ConnectionManager) syncConnections() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.doneCh:
			return
		case uuid, ok := <-m.memberAddCh:
			if !ok {
				return
			}
			m.checkAddConnection(context.Background(), uuid)
		case uuid, ok := <-m.memberRemoveCh:
			if !ok {
				return
			}
			m.checkRemoveConnection(uuid)
		case <-ticker.C:
			m.syncMemberConnections()
		}
	}
}

func (m *ConnectionManager) syncMemberConnections() {
	// add connections to missing members
	for _, addr := range m.clusterService.MemberAddrs() {
		if conn := m.connMap.GetConnectionForAddr(addr); conn == nil {
			// there is no connection to addr
			if member := m.clusterService.GetMemberByAddress(addr); member != nil {
				m.logger.Trace(func() string {
					return fmt.Sprintf("found missing connection to: %s", member.UUID)
				})
				m.memberAddCh <- member.UUID
			}
		}
	}
	// remove connections to non-existent members`
	for _, conn := range m.connMap.Connections() {
		var member *pubcluster.MemberInfo
		var uuid = conn.memberUUID
		if uuid.Default() {
			// this is the very first connection, we don't know the corresponding member UUID
			member = m.clusterService.GetMemberByAddress(conn.Endpoint())
		} else {
			member = m.clusterService.GetMemberByUUID(uuid)
		}
		if member == nil {
			m.logger.Trace(func() string {
				return fmt.Sprintf("found extra connection to: %s", conn)
			})
			m.memberRemoveCh <- conn
		}
	}
}

func (m *ConnectionManager) checkAddConnection(ctx context.Context, uuid types.UUID) {
	m.logger.Trace(func() string {
		return fmt.Sprintf("cluster.ConnectionManager.checkAddConnection: %s", uuid.String())
	})
	member, addr, ok := m.memberAndAddrFor(uuid)
	if !ok {
		return
	}
	if conn := m.connMap.GetConnectionForAddr(addr); conn == nil {
		// there is no connection to addr
		m.logger.Debug(func() string {
			return fmt.Sprintf("trying to connect to: %s", addr)
		})
		if _, err := connectMember(ctx, m, addr, member); err != nil {
			m.logger.Debug(func() string {
				return fmt.Sprintf("could not connect to %s: %s", addr, err.Error())
			})
			// not adding the member back to the candidate list, it should be picked up by syncMemberConnections
		}
	}
}

func (m *ConnectionManager) checkRemoveConnection(conn *Connection) {
	m.logger.Trace(func() string {
		return fmt.Sprintf("cluster.ConnectionManager.checkRemoveConnection: %d", conn.connectionID)
	})
	conn.close(nil)
	m.removeConnection(conn)
}

func (m *ConnectionManager) memberAndAddrFor(uuid types.UUID) (*pubcluster.MemberInfo, pubcluster.Address, bool) {
	// check whether there is a member
	member := m.clusterService.GetMemberByUUID(uuid)
	if member == nil {
		m.logger.Trace(func() string {
			return fmt.Sprintf("cluster.ConnectionManager: member not found for %s", uuid.String())
		})
		return nil, "", false
	}
	addr, err := m.clusterService.MemberAddr(member)
	if err != nil {
		m.logger.Trace(func() string {
			return fmt.Sprintf("cluster.ConnectionManager: error getting address from member: %s", err.Error())
		})
		return nil, "", false
	}
	return member, addr, true
}

type connectionMap struct {
	lb pubcluster.LoadBalancer
	mu *sync.RWMutex
	// addrToConn maps connection address to connection
	addrToConn map[pubcluster.Address]*Connection
	// connToAddr maps connection ID to address
	connToAddr map[int64]pubcluster.Address
	addrs      []pubcluster.Address
}

func newConnectionMap(lb pubcluster.LoadBalancer) *connectionMap {
	return &connectionMap{
		lb:         lb,
		mu:         &sync.RWMutex{},
		addrToConn: map[pubcluster.Address]*Connection{},
		connToAddr: map[int64]pubcluster.Address{},
	}
}

func (m *connectionMap) Reset() {
	m.mu.Lock()
	m.addrToConn = map[pubcluster.Address]*Connection{}
	m.connToAddr = map[int64]pubcluster.Address{}
	m.mu.Unlock()
}

func (m *connectionMap) AddConnection(conn *Connection, addr pubcluster.Address) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// if the connection was already added, skip it
	if _, ok := m.connToAddr[conn.connectionID]; ok {
		return
	}
	m.addrToConn[addr] = conn
	m.connToAddr[conn.connectionID] = addr
	m.addrs = append(m.addrs, addr)
}

// RemoveConnection removes a connection and returns the number of remaining connections.
func (m *connectionMap) RemoveConnection(removedConn *Connection) int {
	m.mu.Lock()
	var remaining int
	for addr, conn := range m.addrToConn {
		if conn.connectionID == removedConn.connectionID {
			delete(m.addrToConn, addr)
			delete(m.connToAddr, conn.connectionID)
			m.removeAddr(addr)
			break
		}
	}
	remaining = len(m.addrToConn)
	m.mu.Unlock()
	return remaining
}

func (m *connectionMap) CloseAll(err error) {
	m.mu.RLock()
	for _, conn := range m.addrToConn {
		conn.close(err)
	}
	m.mu.RUnlock()
}

func (m *connectionMap) GetConnectionForAddr(addr pubcluster.Address) *Connection {
	m.mu.RLock()
	conn := m.addrToConn[addr]
	m.mu.RUnlock()
	return conn
}

func (m *connectionMap) GetAddrForConnectionID(connID int64) (pubcluster.Address, bool) {
	m.mu.RLock()
	addr, ok := m.connToAddr[connID]
	m.mu.RUnlock()
	return addr, ok
}

func (m *connectionMap) RandomConn() *Connection {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.addrs) == 0 {
		return nil
	}
	var addr pubcluster.Address
	if len(m.addrs) == 1 {
		addr = m.addrs[0]
	} else {
		// load balancer mutates its own state
		// so OneOf should be called under write lock
		addr = m.lb.OneOf(m.addrs)
	}
	conn := m.addrToConn[addr]
	if conn != nil && atomic.LoadInt32(&conn.status) == open {
		return conn
	}
	// if the connection was not found by using the load balancer, select the first open one.
	for _, conn = range m.addrToConn {
		// Go randomizes maps, this is random enough.
		if atomic.LoadInt32(&conn.status) == open {
			return conn
		}
	}
	return nil
}

func (m *connectionMap) Connections() []*Connection {
	m.mu.RLock()
	conns := make([]*Connection, 0, len(m.addrToConn))
	for _, conn := range m.addrToConn {
		conns = append(conns, conn)
	}
	m.mu.RUnlock()
	return conns
}

func (m *connectionMap) IsEmpty() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.connToAddr) == 0
}

func (m *connectionMap) FindAddedAddrs(members []pubcluster.MemberInfo, cs *Service) []pubcluster.Address {
	m.mu.RLock()
	addedAddrs := make([]pubcluster.Address, 0, len(members))
	for _, member := range members {
		addr, err := cs.MemberAddr(&member)
		if err != nil {
			continue
		}
		if _, exists := m.addrToConn[addr]; !exists {
			addedAddrs = append(addedAddrs, addr)
		}
	}
	m.mu.RUnlock()
	return addedAddrs
}

func (m *connectionMap) FindRemovedConns(members []pubcluster.MemberInfo) []*Connection {
	m.mu.RLock()
	removedConns := []*Connection{}
	for _, member := range members {
		addr := member.Address
		if conn, exists := m.addrToConn[addr]; exists {
			removedConns = append(removedConns, conn)
		}
	}
	m.mu.RUnlock()
	return removedConns
}

func (m *connectionMap) Info(infoFun func(connections map[pubcluster.Address]*Connection, connToAddr map[int64]pubcluster.Address)) {
	m.mu.RLock()
	infoFun(m.addrToConn, m.connToAddr)
	m.mu.RUnlock()
}

func (m *connectionMap) Len() int {
	m.mu.RLock()
	l := len(m.connToAddr)
	m.mu.RUnlock()
	return l
}

func (m *connectionMap) removeAddr(addr pubcluster.Address) {
	for i, a := range m.addrs {
		if a.Equal(addr) {
			// note that this changes the order of addresses
			m.addrs[i] = m.addrs[len(m.addrs)-1]
			m.addrs = m.addrs[:len(m.addrs)-1]
			break
		}
	}
}

func nonRetryableConnectionErr(err error) bool {
	var ne cb.NonRetryableError
	return ne.Is(err) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func connectMember(ctx context.Context, m *ConnectionManager, addr pubcluster.Address, member *pubcluster.MemberInfo) (pubcluster.Address, error) {
	var initialAddr pubcluster.Address
	var err error
	if _, err = m.ensureConnection(ctx, addr, member); err != nil {
		m.logger.Errorf("cannot connect to %s: %w", addr.String(), err)
	} else if initialAddr == "" {
		initialAddr = addr
	}
	if initialAddr == "" {
		return "", fmt.Errorf("cannot connect to address in the cluster: %w", err)
	}
	return initialAddr, nil
}

func tryConnectAddress(ctx context.Context, m *ConnectionManager, pr pubcluster.PortRange, addr pubcluster.Address, connMember connectMemberFunc) (pubcluster.Address, error) {
	host, port, err := internal.ParseAddr(addr.String())
	if err != nil {
		return "", err
	}
	var initialAddr pubcluster.Address
	if port == 0 { // we need to try all addresses in port range
		for _, currAddr := range util.GetAddresses(host, pr) {
			currentAddrRet, connErr := connMember(ctx, m, currAddr, nil)
			if connErr == nil {
				initialAddr = currentAddrRet
				break
			} else {
				err = connErr
			}
		}
	} else {
		initialAddr, err = connMember(ctx, m, addr, nil)
	}
	if initialAddr == "" {
		return initialAddr, fmt.Errorf("cannot connect to any address in the cluster: %w", err)
	}
	return initialAddr, nil
}
