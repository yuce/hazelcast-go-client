// +build hazelcast_internal

package hazelcast

/*
The API in this file is experimental and can be changed or removed any time.
*/

import (
	"context"
	"encoding/json"

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type Gateway struct {
	proxy *proxy
}

func NewGatewayForClient(c *Client) *Gateway {
	return &Gateway{proxy: c.proxyManager.invocationProxy}
}

func (g *Gateway) Encode(obj interface{}) ([]byte, error) {
	data, err := g.proxy.convertToData(obj)
	if err != nil {
		return nil, err
	}
	return data.Payload, nil
}

func (g *Gateway) Decode(b []byte) (interface{}, error) {
	data := serialization.Data{Payload: b}
	o, err := g.proxy.convertToObject(&data)
	if err != nil {
		return nil, err
	}
	return o, nil
}

func (g *Gateway) InvokeRandom(ctx context.Context, request []byte) ([]byte, error) {
	var r proto.ClientMessage
	if err := json.Unmarshal(request, &r); err != nil {
		return nil, err
	}
	res, err := g.proxy.invokeOnRandomTarget(ctx, &r, nil)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	return b, nil
}
