package hazelcast

import (
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	icluster "github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
)

type Config struct {
	ClientName  string
	ClusterName string
	Network     cluster.NetworkConfig
}

type ConfigProvider interface {
	Config() (Config, error)
}

type ConfigBuilder interface {
	SetClientName(name string) ConfigBuilder
	SetClusterName(name string) ConfigBuilder
	Network() cluster.NetworkConfigBuilder
	Config() (Config, error)
}

type configBuilderImpl struct {
	config               Config
	networkConfigBuilder *icluster.NetworkConfigBuilderImpl
}

func newConfigBuilderImpl() *configBuilderImpl {
	return &configBuilderImpl{
		networkConfigBuilder: icluster.NewNetworkConfigBuilderImpl(),
	}
}

func (c *configBuilderImpl) SetClientName(name string) ConfigBuilder {
	c.config.ClientName = name
	return c
}

func (c *configBuilderImpl) SetClusterName(name string) ConfigBuilder {
	c.config.ClusterName = name
	return c
}

func (c *configBuilderImpl) Network() cluster.NetworkConfigBuilder {
	if c.networkConfigBuilder == nil {
		c.networkConfigBuilder = &icluster.NetworkConfigBuilderImpl{}
	}
	return c.networkConfigBuilder
}

func (c configBuilderImpl) Config() (Config, error) {
	if c.networkConfigBuilder != nil {
		if networkConfig, err := c.networkConfigBuilder.Config(); err != nil {
			return Config{}, err
		} else {
			c.config.Network = networkConfig
		}
	}
	return c.config, nil
}