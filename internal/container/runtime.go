package container

import (
	"context"
	"fmt"
	"strings"

	"github.com/docker/docker/api/types"
	containerapi "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Runtime interface {
	LaunchContainer(ctx context.Context, name string, env map[string]string) (Container, error)
	ListContainers(ctx context.Context, prefix string) ([]Container, error)
	TerminateContainer(container Container) error
}

type DockerRuntime struct {
	opts LaunchContainerOpts
	cli  *client.Client
	log  *zerolog.Logger
}

type LaunchContainerOpts struct {
	Image string
	Port  int
}

func NewDockerRuntime(opts LaunchContainerOpts) (Runtime, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}
	logger := log.With().
		Str("me", "docker-runtime").
		Logger()
	return &DockerRuntime{
		opts: opts,
		cli:  cli,
		log:  &logger,
	}, nil
}

func (r *DockerRuntime) LaunchContainer(
	ctx context.Context,
	name string,
	env map[string]string,
) (Container, error) {
	var envArgs []string
	for k, v := range env {
		envArgs = append(envArgs, k+"="+v)
	}

	body, err := r.cli.ContainerCreate(ctx,
		&containerapi.Config{
			Image: r.opts.Image,
			Env:   envArgs,
		},
		&containerapi.HostConfig{
			RestartPolicy: containerapi.RestartPolicy{
				Name: "always",
			},
			PortBindings: nat.PortMap{
				nat.Port(fmt.Sprintf("%d/tcp", r.opts.Port)): []nat.PortBinding{
					{HostIP: "127.0.0.1", HostPort: "0"},
				},
			},
		},
		nil, nil, name)
	if err != nil {
		return nil, fmt.Errorf("failed to create container: %w", err)
	}

	defer func() {
		if err != nil {
			r.cli.ContainerRemove(context.Background(), body.ID, types.ContainerRemoveOptions{
				Force: true,
			})
		}
	}()

	err = r.cli.ContainerStart(ctx, body.ID, types.ContainerStartOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	addr, err := r.getContainerAddr(ctx, body.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get container addr: %w", err)
	}
	return NewContainer(r, Config{
		Name: name, ID: body.ID, Addr: addr,
	}), nil
}

func (r *DockerRuntime) ListContainers(ctx context.Context, prefix string) ([]Container, error) {
	list, err := r.cli.ContainerList(ctx, types.ContainerListOptions{})
	if err != nil {
		return nil, err
	}
	var containers []Container
	for _, container := range list {
		if len(container.Names) != 1 {
			continue
		}
		name := strings.Trim(container.Names[0], "/")
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		addr, err := r.getContainerAddr(ctx, container.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get container addr: %w", err)
		}
		containers = append(containers, NewContainer(r, Config{
			Name: name, ID: container.ID, Addr: addr,
		}))
	}
	return containers, nil
}

func (r *DockerRuntime) getContainerAddr(ctx context.Context, id string) (string, error) {
	descr, err := r.cli.ContainerInspect(ctx, id)
	if err != nil {
		return "", err
	}
	containerPort := nat.Port(fmt.Sprintf("%d/tcp", r.opts.Port))
	bindings := descr.NetworkSettings.Ports[containerPort]
	return "127.0.0.1:" + bindings[0].HostPort, nil
}

func (r *DockerRuntime) TerminateContainer(c Container) error {
	if err := r.cli.ContainerStop(context.TODO(), c.ID(), nil); err != nil {
		r.log.Warn().
			Str("container", c.Name()).
			Msg("Failed to gracefully stop container")
	}
	return r.cli.ContainerRemove(context.TODO(), c.ID(), types.ContainerRemoveOptions{
		Force: true,
	})
}
