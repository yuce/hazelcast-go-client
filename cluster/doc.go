/*
Package cluster contains Hazelcast cluster related data types and functions.

Configuring Load Balancer

Load balancer configuration allows you to specify which cluster address to send next operation.

If smart client mode is used, only the operations that are not key-based are routed to the member that is returned by the load balancer.
Load balancer is ignored for unisocket mode.

The default load balancer is the RoundRobinLoadBalancer, which picks the next address in order among the provided addresses.
The other built-in load balancer is RandomLoadBalancer.
You can also write a custom load balancer by implementing LoadBalancer.

Use config.ClusterConfig.LoadBalancer to set the load balancer:

	config := NewConfig()
	config.ClusterConfig.LoadBalancer = cluster.NewRandomLoadBalancer()

Hazelcast Cloud Discovery

Hazelcast Go client can discover and connect to Hazelcast clusters running on Hazelcast Cloud https://cloud.hazelcast.com.
In order to activate it, set the cluster name, enable Hazelcast Cloud discovery and add Hazelcast Cloud Token to the configuration.
Here is an example:

	config := hazelcast.NewConfig()
	config.ClusterConfig.Name = "MY-CLUSTER-NAME"
	cc := &config.ClusterConfig.HazelcastCloudConfig
	cc.Enabled = true
	cc.Token = "MY-CLUSTER-TOKEN"
	client, err := hazelcast.StartNewClientWithConfig(config)
	if err != nil {
		log.Fatal(err)
	}

Also check the code sample in https://github.com/hazelcast/hazelcast-go-client/tree/master/examples/discovery/cloud.

If you have enabled encryption for your cluster, you should also enable TLS/SSL configuration for the client.

External Client Public Address Discovery

When you set up a Hazelcast cluster in the Cloud (AWS, Azure, GCP, Kubernetes) and would like to use it from outside the Cloud network,
the client needs to communicate with all cluster members via their public IP addresses.
Whenever Hazelcast cluster members are able to resolve their own public external IP addresses, they pass this information to the client.
As a result, the client can use public addresses for communication, if it cannot access members via private IPs.

Hazelcast Go client has a built-in mechanism to use public IP addresses instead of private ones.
You can enable this feature by setting config.DiscoveryConfig.UsePublicIP to true and specifying the adddress of at least one member:

	config := hazelcast.NewConfig()
	cc := &config.ClusterConfig
	cc.SetAddress("30.40.50.60:5701")
	cc.DiscoveryConfig.UsePublicIP = true

For more details on member-side configuration, refer to the Discovery SPI section in the Hazelcast IMDG Reference Manual.

*/
package cluster
