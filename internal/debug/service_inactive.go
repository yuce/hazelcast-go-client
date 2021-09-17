// +build !hazelcast_debug

package debug

import "github.com/hazelcast/hazelcast-go-client/internal/logger"

func NewService(logger logger.Logger) *Service {
	return &Service{}
}

func (s *Service) Stop() {
	// pass
}
