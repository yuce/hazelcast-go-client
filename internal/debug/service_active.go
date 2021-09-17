// +build hazelcast_debug

package debug

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

func NewService(logger logger.Logger) *Service {
	s := &Service{
		buf:    make([]byte, 10*1024*1024), // 10 MB
		logger: logger,
		doneCh: make(chan struct{}),
	}
	if logger.CanLogDebug() {
		go s.logDebug()
	}
	return s
}

func (s *Service) Stop() {
	if !atomic.CompareAndSwapInt32(&s.state, 0, 1) {
		return
	}
	close(s.doneCh)
}

func (s *Service) logDebug() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.doneCh:
			return
		case <-ticker.C:
			s.logGoroutines()
		}
	}
}

func (s *Service) logGoroutines() {
	s.logger.Trace(func() string {
		return fmt.Sprintf("\n\nGoroutines at %v:\n\n", time.Now())
	})
	n := runtime.Stack(s.buf, true)
	s.logger.Trace(func() string {
		return fmt.Sprintf("%s\n", string(s.buf[:n]))
	})
}
