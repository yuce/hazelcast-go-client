package cloud

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
)

func TestExtractAddresses(t *testing.T) {
	s := `[{"private-address":"100.115.50.221","public-address":"35.177.212.248:31984"},{"private-address":"100.109.198.133","public-address":"3.8.123.82:31984"}]`
	r := []interface{}{}
	if err := json.Unmarshal([]byte(s), &r); err != nil {
		t.Fatal(err)
	}
	addrs := extractAddresses(r)
	target := []Address{
		NewAddress("35.177.212.248:31984", "100.115.50.221:31984"),
		NewAddress("3.8.123.82:31984", "100.109.198.133:31984"),
	}
	assert.Equal(t, target, addrs)
}

func TestNormalizePrivatePublicAddr(t *testing.T) {
	testCases := []struct {
		Pr  string
		Pu  string
		TPr string
		TPu string
	}{
		{Pr: "100.109.198.133", Pu: "3.8.123.82:31984", TPr: "100.109.198.133:31984", TPu: "3.8.123.82:31984"},
		{Pr: "100.109.198.133:5555", Pu: "3.8.123.82:31984", TPr: "100.109.198.133:5555", TPu: "3.8.123.82:31984"},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			priv, pub := normalizePrivatePublicAddr(tc.Pr, tc.Pu)
			assert.Equal(t, tc.TPr, priv)
			assert.Equal(t, tc.TPu, pub)
		})
	}
}

func TestTranslateAddrs(t *testing.T) {
	testCases := []struct {
		E  string
		CA []Address
		A  []pubcluster.Address
	}{
		{CA: []Address{}, A: []pubcluster.Address{}},
		{
			CA: []Address{
				{Public: "30.40.50.60:1234", Private: "100.101.102.13:1234"},
				{Public: "40.40.50.60:1234", Private: "100.101.102.13:1234"},
			},
			A: []pubcluster.Address{"30.40.50.60:1234", "40.40.50.60:1234"},
		},
		{
			CA: []Address{{Public: "30.40.50.60"}},
			E:  "parsing address: address 30.40.50.60: missing port in address",
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			addrs, err := translateAddrs(tc.CA)
			if tc.E != "" {
				if err == nil {
					t.Fatalf("should have failed")
				}
				assert.Equal(t, tc.E, err.Error())
				return
			}
			assert.Equal(t, tc.A, addrs)
		})
	}
}

func TestMakeCoordinatorURL(t *testing.T) {
	url := makeCoordinatorURL("TOK")
	target := "https://coordinator.hazelcast.cloud/cluster/discovery?token=TOK"
	assert.Equal(t, target, url)
	if err := os.Setenv(envCoordinatorBaseURL, "http://test.dev"); err != nil {
		t.Fatal(err)
	}
	url = makeCoordinatorURL("TOK")
	target = "http://test.dev/cluster/discovery?token=TOK"
	assert.Equal(t, target, url)
}
