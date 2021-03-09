package client

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
    "github.com/hazelcast/hazelcast-go-client/v4/config"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/v4/security"
	"github.com/hazelcast/hazelcast-go-client/v4/serialization"
	"sync/atomic"
	"time"
)

var nextId int32


type Impl struct {
	// configuration
	name          string
	clusterName   string
	networkConfig *config.NetworkConfig

	// components
	proxyManager proxy.Manager
	//serializationService spi.SerializationService
	//partitionService     cluster.PartitionService
	//invocationService    invocation.Service
	//clusterService       cluster.Service
	connectionManager cluster.ConnectionManager
	logger            logger.Logger

	// state
	started atomic.Value
}

func NewImpl(name string, config *config.Config) *Impl {
	id := atomic.AddInt32(&nextId, 1)
	if name == "" {
		name = fmt.Sprintf("hz.client_%d", id)
	}
	// TODO: consider disabling manual client name
	if config.ClientName() != "" {
		name = config.ClientName()
	}
	clientLogger := logger.New()
	// TODO: create services
	serializationService, err := serialization.NewService(serialization.NewConfig())
	if err != nil {
		panic(fmt.Errorf("error creating client: %w", err))
	}
	smartRouting := config.NetworkConfig().IsSmartRouting()
	addressTranslator := cluster.NewDefaultAddressTranslator()
	addressProviders := []cluster.AddressProvider{
		cluster.NewDefaultAddressProvider(config.NetworkConfig()),
	}
	credentials := security.NewUsernamePasswordCredentials("", "")
	clusterService := cluster.NewServiceImpl(addressProviders)
	partitionService := cluster.NewPartitionServiceImpl(cluster.PartitionServiceCreationBundle{
		SerializationService: serializationService,
		Logger:               clientLogger,
	})
	invocationService := invocation.NewServiceImpl(invocation.ServiceCreationBundle{
		SmartRouting: smartRouting,
		Logger:       clientLogger,
	})
	connectionManager := cluster.NewConnectionManagerImpl(cluster.ConnectionManagerCreationBundle{
		SmartRouting:         smartRouting,
		Logger:               clientLogger,
		AddressTranslator:    addressTranslator,
		InvocationService:    invocationService,
		ClusterService:       clusterService,
		PartitionService:     partitionService,
		SerializationService: serializationService,
		NetworkConfig:        config.NetworkConfig(),
		Credentials:          credentials,
		ClientName:           name,
	})
	invocationHandler := cluster.NewConnectionInvocationHandler(cluster.ConnectionInvocationHandlerCreationBundle{
		ConnectionManager: connectionManager,
		ClusterService:    clusterService,
		SmartRouting:      smartRouting,
		Logger:            clientLogger,
	})
	invocationService.SetHandler(invocationHandler)
	invocationFactory := cluster.NewConnectionInvocationFactory(partitionService, 120*time.Second)
	proxyManagerServiceBundle := proxy.ProxyCreationBundle{
		SerializationService: serializationService,
		PartitionService:     partitionService,
		InvocationService:    invocationService,
		ClusterService:       clusterService,
		SmartRouting:         smartRouting,
		InvocationFactory:    invocationFactory,
	}
	impl := &Impl{
		name:              name,
		clusterName:       config.GroupConfig().Name(), //TODO this should be cluster name
		networkConfig:     config.NetworkConfig(),
		proxyManager:      proxy.NewManagerImpl(proxyManagerServiceBundle),
		connectionManager: connectionManager,
		//invocationService: invocationService,
		logger: clientLogger,
	}
	impl.started.Store(false)
	return impl
}

func (c *Impl) Name() string {
	return c.name
}

func (c *Impl) GetMap(name string) (hazelcast.Map, error) {
	c.ensureStarted()
	return c.proxyManager.GetMap(name)
}


func (c *Impl) ensureStarted() {
	if c.started.Load() == false {
		panic("client not started")
	}
}

func (c *Impl) Start() error {
	// TODO: Recover from panics and return as error
	if c.started.Load() == true {
		return nil
	}
	if err := c.connectionManager.Start(); err != nil {
		return err
	}
	c.started.Store(true)
	return nil
}

/*
func (c ConnectionImpl) ClusterName() string {
	return c.clusterName
}

func (c ConnectionImpl) NetworkConfig() *NetworkConfig {
	return c.networkConfig
}
*/
