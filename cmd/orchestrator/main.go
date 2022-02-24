package main

import (
	"context"
	"errors"
	"flag"
	"milabs-orchestrator/internal/api"
	"milabs-orchestrator/internal/container"
	"milabs-orchestrator/internal/pool"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	listneAddr           = flag.String("listen-addr", ":8080", "")
	containerImage       = flag.String("container-img", "quay.io/milaboratory/qual-2021-devops-server", "")
	containerPort        = flag.Int("container-port", 8080, "")
	containerIdleTimeout = flag.Duration("idle-timeout", time.Minute*2, "")
)

func main() {
	flag.Parse()

	runtime, err := container.NewDockerRuntime(container.LaunchContainerOpts{
		Image: *containerImage,
		Port:  *containerPort,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to configure docker runtime")
	}

	workerPool, err := pool.NewManager(pool.ManagerOpts{
		IdleTimeout: *containerIdleTimeout,
	}, runtime)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to configure container pool manager")
	}

	srv := api.NewServer(*listneAddr, workerPool)

	log.Info().Msg("Starting api server")
	go func() {
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("Failed to start api server")
		}
	}()

	log.Info().Msg("Orchestrator launched")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	log.Info().Msg("Shutting down api server")
	srv.Shutdown(context.Background())

	log.Info().Msg("Shutting down container pool")
	workerPool.Shutdown()
}
