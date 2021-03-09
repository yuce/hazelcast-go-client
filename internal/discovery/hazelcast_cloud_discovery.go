// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package discovery

import (
	"net/http"

	"encoding/json"

	"io/ioutil"

	"time"

	"crypto/tls"
	"crypto/x509"

	"github.com/hazelcast/hazelcast-go-client/v4/config/property"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/iputil"
)

const (
	cloudURLPath = "/cluster/discovery?token="
)

var CloudURLBaseProperty = property.NewHazelcastPropertyString("hazelcast.client.cloud.url",
	"https://coordinator.hazelcast.cloud")

type nodeDiscoverer func() (map[string]*hazelcast.Address, error)

type addr struct {
	PrivAddr   string `json:"private-address"`
	PublicAddr string `json:"public-address"`
}

type HazelcastCloud struct {
	client            http.Client
	endPointURL       string
	connectionTimeout time.Duration
	discoverNodes     nodeDiscoverer
}

func NewHazelcastCloud(endpointURL string, connectionTimeout time.Duration, certPool *x509.CertPool) *HazelcastCloud {
	hzCloud := &HazelcastCloud{}
	hzCloud.client = http.Client{
		Timeout: hzCloud.connectionTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: certPool,
			},
		},
	}
	hzCloud.endPointURL = endpointURL
	hzCloud.connectionTimeout = connectionTimeout
	hzCloud.discoverNodes = hzCloud.discoverNodesInternal
	return hzCloud
}

func (hzC *HazelcastCloud) discoverNodesInternal() (map[string]*hazelcast.Address, error) {
	return hzC.callService()
}

func (hzC *HazelcastCloud) callService() (map[string]*hazelcast.Address, error) {
	url := hzC.endPointURL
	resp, err := hzC.client.Get(url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, hazelcast.NewHazelcastIOError("got a status :"+resp.Status, nil)
	}
	// Check certificates
	if !hzC.checkCertificates(resp) {
		return nil, hazelcast.NewHazelcastCertificateError("invalid certificate from hazelcast.cloud endpoint",
			nil)
	}

	return hzC.parseResponse(resp)
}

func (hzC *HazelcastCloud) checkCertificates(response *http.Response) bool {
	if response.TLS == nil {
		return false
	}
	for _, cert := range response.TLS.PeerCertificates {
		if !cert.BasicConstraintsValid {
			return false
		}
	}
	return true
}

func (hzC *HazelcastCloud) parseResponse(response *http.Response) (map[string]*hazelcast.Address, error) {
	var target = make([]addr, 0)
	var privateToPublicAddrs = make(map[string]*hazelcast.Address)
	resp, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(resp, &target); err != nil {
		return nil, err
	}

	for _, addr := range target {
		publicAddress := hzC.createAddress(addr.PublicAddr)
		// TODO:: what if privateAddress is not okay ?
		// TODO:: use addressProvider
		privateAddress := hzC.createAddress(addr.PrivAddr)
		if privateAddress.Port() == -1 {
			privateAddress = hazelcast.NewAddressWithHostPort(addr.PrivAddr, publicAddress.Port())
		}

		privateToPublicAddrs[privateAddress.String()] = publicAddress
	}

	return privateToPublicAddrs, nil
}

func CreateURLEndpoint(hazelcastProperties *property.HazelcastProperties, cloudToken string) string {
	cloudBaseURL := hazelcastProperties.GetString(CloudURLBaseProperty)
	return cloudBaseURL + cloudURLPath + cloudToken
}

func (hzC *HazelcastCloud) createAddress(hostname string) *hazelcast.Address {
	ip, port := iputil.GetIPAndPort(hostname)
	return hazelcast.NewAddressWithHostPort(ip, port)
}
