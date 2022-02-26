package container

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.uber.org/atomic"
)

type Container interface {
	Process(ctx context.Context, input string) (*ComputeResult, error)
	Watch() <-chan State
	State() State
	Shutdown()
	Name() string
	ID() string
}

type State int32

const (
	Starting = iota
	Idle
	Processing
	Finished
)

type Config struct {
	Name string
	ID   string
	Addr string
}

type container struct {
	cfg     Config
	client  *http.Client
	runtime Runtime
	log     *zerolog.Logger

	mu       sync.Mutex
	requests map[string]*processRequest

	state           *atomic.Int32
	events          chan State
	shutdown, ready chan struct{}
}

type ComputeResult struct {
	Data []byte
	Code int
	Err  error
}

type processRequest struct {
	done    chan struct{}
	clients sync.WaitGroup
	result  *ComputeResult
}

const (
	apiEndpoint    = "/calculate"
	healthEndpoint = "/health"
)

func NewContainer(runtime Runtime, cfg Config) Container {
	logger := log.With().
		Str("me", "container").
		Str("name", cfg.Name).
		Logger()
	c := &container{
		cfg:      cfg,
		client:   &http.Client{},
		runtime:  runtime,
		requests: make(map[string]*processRequest),
		events:   make(chan State, 1),
		shutdown: make(chan struct{}),
		ready:    make(chan struct{}),
		state:    atomic.NewInt32(-1),
		log:      &logger,
	}
	c.setState(Starting)
	go c.initialize()
	return c
}

func (c *container) initialize() {
	defer c.setState(Idle)
	defer close(c.ready)

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-c.shutdown:
			return
		default:
		}
		c.log.Info().Msg("Waiting for container to initialize...")

		resp, err := c.client.Get("http://" + c.cfg.Addr + healthEndpoint)
		if err != nil {
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == 200 {
			break
		}
	}
	c.log.Info().Msg("Container initialized")
}

func (c *container) setState(s State) {
	if c.State() == s {
		return
	}
	c.state.Store(int32(s))
	select {
	case c.events <- s:
	default:
	}
}

func (c *container) ID() string { return c.cfg.ID }

func (c *container) Name() string { return c.cfg.Name }

func (c *container) State() State {
	return State(c.state.Load())
}

func (c *container) Watch() <-chan State {
	return c.events
}

func (c *container) Process(ctx context.Context, input string) (*ComputeResult, error) {
	if err := c.waitReady(ctx); err != nil {
		return nil, err
	}

	req := c.getOrMakeRequest(input)
	defer req.clients.Done()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-req.done:
	}
	return req.result, nil
}

func (c *container) waitReady(ctx context.Context) error {
	if c.State() == Starting {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.ready:
		}
	}
	return nil
}

func (c *container) getOrMakeRequest(input string) *processRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	req := c.requests[input]
	if req == nil {
		c.log.Info().
			Str("input", input).
			Msg("Making new request")

		req = c.makeRequest(input)
		c.requests[input] = req
	} else {
		c.log.Info().
			Str("input", input).
			Msg("Reusing existing request")

		req.clients.Add(1)
	}
	return req
}

func (c *container) makeRequest(input string) *processRequest {
	req := &processRequest{
		done:   make(chan struct{}),
		result: &ComputeResult{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	req.clients.Add(1)

	c.setState(Processing)
	go func() {
		req.clients.Wait()
		cancel()
		c.removeRequest(input)

		if req.result.Err != nil || req.result.Code != 200 {
			c.log.Error().
				Err(req.result.Err).
				Int("code", req.result.Code).
				Str("input", input).
				Msg("Request failed")
		} else {
			c.log.Info().
				Str("input", input).
				Int("code", req.result.Code).
				Msg("Request succeeded")
		}
	}()

	go func() {
		defer close(req.done)

		url := "http://" + c.cfg.Addr + apiEndpoint + "/" + input
		httpReq, _ := http.NewRequestWithContext(ctx, "", url, nil)

		resp, err := c.client.Do(httpReq)
		if err != nil {
			req.result.Code = 500
			req.result.Err = err
			return
		}
		defer resp.Body.Close()

		data, err := io.ReadAll(resp.Body)
		if err != nil {
			req.result.Err = err
			return
		}
		req.result.Code = resp.StatusCode
		req.result.Data = data
	}()
	return req
}

func (c *container) removeRequest(input string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.requests, input)
	if len(c.requests) == 0 {
		c.setState(Idle)
	}
}

func (c *container) Shutdown() {
	defer c.setState(Finished)
	c.log.Info().Msg("Shutting container down")

	close(c.shutdown)
	wg := sync.WaitGroup{}
	c.mu.Lock()
	for _, req := range c.requests {
		req := req
		wg.Add(1)
		go func() {
			req.clients.Wait()
			wg.Done()
		}()
	}
	c.mu.Unlock()
	wg.Wait()

	err := c.runtime.TerminateContainer(c)
	if err != nil {
		c.log.Error().
			Err(err).
			Msg("Failed to remove container")
	}
	c.log.Info().Msg("Container finished")
}
