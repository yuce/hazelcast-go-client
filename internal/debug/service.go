package debug

import "github.com/hazelcast/hazelcast-go-client/internal/logger"

type Service struct {
	buf    []byte
	logger logger.Logger
	doneCh chan struct{}
	state  int32
}
