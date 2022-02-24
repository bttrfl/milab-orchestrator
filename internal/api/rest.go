package api

import (
	"milabs-orchestrator/internal/pool"
	"net/http"

	"github.com/gorilla/mux"
	lru "github.com/hashicorp/golang-lru"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Server struct {
	*http.Server
	workerPool pool.Manager
	cache      *lru.Cache
	log        *zerolog.Logger
}

func NewServer(addr string, workerPool pool.Manager) *Server {
	logger := log.With().Str("me", "rest-srv").Logger()
	cache, _ := lru.New(1024)
	srv := &Server{
		Server: &http.Server{
			Addr: addr,
		},
		workerPool: workerPool,
		cache:      cache,
		log:        &logger,
	}
	r := mux.NewRouter()
	r.HandleFunc("/calculate/{seed}/{input}", srv.calculateHandler)
	srv.Handler = r
	return srv
}

func (s *Server) calculateHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	seed, input := vars["seed"], vars["input"]

	logger := s.log.With().
		Str("addr", r.RemoteAddr).
		Str("seed", seed).
		Str("input", input).
		Logger()
	logger.Info().Msg("Handling new request")

	cached, ok := s.cache.Get(seed + "/" + input)
	if ok {
		logger.Info().Msg("Returning result from cache")
		w.Write(cached.([]byte))
		return
	}

	result, err := s.workerPool.Handle(r.Context(), &pool.ComputeRequest{
		Seed:  seed,
		Input: input,
	})
	if err != nil {
		http.Error(w, err.Error(), 500)
		logger.Error().Err(err).Msg("Request failed")
		return
	}

	if result.Code >= 400 {
		logger.Error().Int("code", result.Code).Msg("Request failed")
	} else {
		s.cache.Add(seed+"/"+input, result.Data)
		logger.Info().Msg("Request succeeded")
	}

	w.WriteHeader(result.Code)
	w.Write(result.Data)
}
