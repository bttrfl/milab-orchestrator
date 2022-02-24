package pool

import (
	"context"
	"fmt"
	"milabs-orchestrator/internal/container"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Manager interface {
	Handle(ctx context.Context, req *ComputeRequest) (*container.ComputeResult, error)
	Shutdown()
}

type ComputeRequest struct {
	Seed, Input string
}

type manager struct {
	opts    ManagerOpts
	runtime container.Runtime
	log     *zerolog.Logger

	mu         sync.Mutex
	containers map[string]container.Container

	shutdown chan struct{}
	watchers sync.WaitGroup
}

type ManagerOpts struct {
	IdleTimeout time.Duration
}

const containerNamePrefix = "milabs-worker"

func NewManager(opts ManagerOpts, runtime container.Runtime) (Manager, error) {
	logger := log.With().Str("me", "container-manager").Logger()
	m := &manager{
		opts:       opts,
		runtime:    runtime,
		log:        &logger,
		containers: make(map[string]container.Container),
		shutdown:   make(chan struct{}),
	}

	logger.Info().Msg("Recovering containers from prev launch")

	containers, err := m.runtime.ListContainers(context.TODO(), containerNamePrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to init container list: %w", err)
	}
	for _, c := range containers {
		seed := m.getSeedFromName(c.Name())
		if seed == "" {
			continue
		}
		m.watchers.Add(1)
		go m.watchContainer(c, seed)
		m.containers[seed] = c
	}
	return m, nil
}

func (m *manager) getSeedFromName(containerName string) string {
	parts := strings.SplitN(containerName, "-", 3)
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}

func (m *manager) Handle(ctx context.Context, req *ComputeRequest) (*container.ComputeResult, error) {
	c, err := m.getOrCreateContainer(ctx, req.Seed)
	if err != nil {
		return nil, fmt.Errorf("failed to get container for request: %w", err)
	}
	result, err := c.Process(ctx, req.Input)
	if err != nil {
		return nil, fmt.Errorf("compute request failed: %w", err)
	}
	return result, err
}

func (m *manager) getOrCreateContainer(ctx context.Context, seed string) (container.Container, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.containers[seed]
	if ok {
		m.log.Info().
			Str("seed", seed).
			Msg("Reusing existing container")
		return c, nil
	}

	var err error
	name := containerNamePrefix + "-" + seed
	c, err = m.runtime.LaunchContainer(ctx, name, map[string]string{"SEED": seed})
	if err != nil {
		return nil, err
	}
	m.containers[seed] = c

	m.log.Info().
		Str("seed", seed).
		Str("name", name).
		Msg("Launched new container")

	m.watchers.Add(1)
	go m.watchContainer(c, seed)
	return c, nil
}

func (m *manager) Shutdown() {
	close(m.shutdown)
	m.watchers.Wait()
}

func (m *manager) watchContainer(c container.Container, seed string) {
	defer m.watchers.Done()

	logger := log.With().
		Str("me", "container-watcher").
		Str("name", c.Name()).
		Logger()
	logger.Info().Msg("Watching new container")

	var timer *time.Timer
	for {
		var state container.State
		select {
		case <-m.shutdown:
			logger.Info().Msg("Shutting down watcher and container")
			c.Shutdown()
			return
		case state = <-c.Watch():
		}
		switch state {
		case container.Starting:
			logger.Info().Msg("Container is starting")
		case container.Idle:
			logger.Info().Msg("Container is idle, scheduled cleanup task")
			timer = time.AfterFunc(m.opts.IdleTimeout, func() {
				m.mu.Lock()
				delete(m.containers, seed)
				m.mu.Unlock()
				c.Shutdown()
			})
		case container.Processing:
			logger.Info().Msg("Container state changed to processing, canceled cleanup task")
			timer.Stop()
		case container.Finished:
			logger.Info().Msg("Container finished")
			return
		}
	}
}
