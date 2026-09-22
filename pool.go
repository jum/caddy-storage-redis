// Copyright 2024 Pieter Berkel
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storageredis

import (
	"sync"
	"time"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	defaultGracePeriodStr = "30s"
	defaultGracePeriod    = 30 * time.Second
)

type pooledClientEntry struct {
	client      redis.UniversalClient
	locker      *redislock.Client
	refCount    int
	lingerTimer *time.Timer
}

type redisClientPool struct {
	mu      sync.Mutex
	entries map[string]*pooledClientEntry
}

var defaultPool = newRedisClientPool()

func newRedisClientPool() *redisClientPool {
	return &redisClientPool{
		entries: make(map[string]*pooledClientEntry),
	}
}

func (p *redisClientPool) acquire(
	key string,
	safeKey string,
	logger *zap.SugaredLogger,
	factory func() (redis.UniversalClient, *redislock.Client, error),
) (redis.UniversalClient, *redislock.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if entry, exists := p.entries[key]; exists {
		if entry.lingerTimer != nil {
			if entry.lingerTimer.Stop() {
				if logger != nil {
					logger.Debugf("Cancelled delayed shutdown for pooled Redis client (%s)", safeKey)
				}
			}
			entry.lingerTimer = nil
		}
		entry.refCount++
		if logger != nil {
			logger.Debugf("Reused pooled Redis client (%s), refCount: %d", safeKey, entry.refCount)
		}
		return entry.client, entry.locker, nil
	}

	client, locker, err := factory()
	if err != nil {
		return nil, nil, err
	}

	p.entries[key] = &pooledClientEntry{
		client:   client,
		locker:   locker,
		refCount: 1,
	}
	if logger != nil {
		logger.Debugf("Created new pooled Redis client (%s), refCount: 1", safeKey)
	}
	return client, locker, nil
}

func (p *redisClientPool) release(
	key string,
	safeKey string,
	gracePeriod time.Duration,
	logger *zap.SugaredLogger,
) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, exists := p.entries[key]
	if !exists {
		return
	}

	entry.refCount--
	if logger != nil {
		logger.Debugf("Released pooled Redis client (%s), refCount: %d", safeKey, entry.refCount)
	}

	if entry.refCount > 0 {
		return
	}

	// refCount reached 0
	if gracePeriod <= 0 {
		delete(p.entries, key)
		if entry.lingerTimer != nil {
			entry.lingerTimer.Stop()
			entry.lingerTimer = nil
		}
		if err := entry.client.Close(); err != nil && logger != nil {
			logger.Warnf("Error closing Redis client (%s): %v", safeKey, err)
		}
		if logger != nil {
			logger.Debugf("Closed Redis client immediately (%s)", safeKey)
		}
		return
	}

	if entry.lingerTimer != nil {
		entry.lingerTimer.Stop()
	}

	if logger != nil {
		logger.Debugf("Scheduled delayed shutdown for Redis client (%s) in %v", safeKey, gracePeriod)
	}

	entry.lingerTimer = time.AfterFunc(gracePeriod, func() {
		p.mu.Lock()
		currEntry, stillExists := p.entries[key]
		if !stillExists || currEntry != entry || entry.refCount > 0 {
			p.mu.Unlock()
			return
		}
		delete(p.entries, key)
		p.mu.Unlock()

		if err := entry.client.Close(); err != nil && logger != nil {
			logger.Warnf("Error closing Redis client after grace period (%s): %v", safeKey, err)
		}
		if logger != nil {
			logger.Infof("Closed Redis client after grace period (%s)", safeKey)
		}
	})
}

func (p *redisClientPool) getRefCount(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, exists := p.entries[key]; exists {
		return entry.refCount
	}
	return 0
}

func (p *redisClientPool) hasLingerTimer(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, exists := p.entries[key]; exists {
		return entry.lingerTimer != nil
	}
	return false
}

func (p *redisClientPool) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

func (p *redisClientPool) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, entry := range p.entries {
		if entry.lingerTimer != nil {
			entry.lingerTimer.Stop()
		}
		_ = entry.client.Close()
		delete(p.entries, k)
	}
}
