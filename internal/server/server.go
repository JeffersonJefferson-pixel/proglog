package server

import (
	"context"
	api "proglog/api/v1"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	CommitLog CommitLog
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

var _ api.LogServer = (*grpcServer)(nil)

func newGrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{Config: config}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	return nil, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	return nil, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	return nil
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	return nil
}
