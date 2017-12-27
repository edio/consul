package checks

import (
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/health"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"
	"log"
	"net"
	"os"
	"testing"
	"time"
)

var (
	port              int
	stubServerAddress string
	logger            = log.New(os.Stderr, "", log.LstdFlags)
	stubServer        *health.Server
)

func startServer() (*health.Server, net.Listener) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	server := health.NewServer()
	hv1.RegisterHealthServer(grpcServer, server)
	go grpcServer.Serve(listener)
	return server, listener
}

func init() {
	flag.IntVar(&port, "grpc-stub-port", 54321, "port for the gRPC stub server")
}

func TestMain(m *testing.M) {
	flag.Parse()
	stubServerAddress = fmt.Sprintf("%s:%d", "localhost", port)

	srv, lis := startServer()

	stubServer = srv
	result := 0
	defer func() {
		lis.Close()
		os.Exit(result)
	}()

	result = m.Run()
}

func TestGrpcConnectionPool_Acquire_newConnection(t *testing.T) {
	// given
	pool := NewGrpcConnectionPool(logger)
	defer pool.tearDown()

	// when
	connection, err := pool.Acquire(stubServerAddress, context.Background())

	// then
	if err != nil {
		t.Fatalf("error should be nil")
	}
	if connection == nil {
		t.Fatalf("connection should be established")
	}
	if pool.connections[stubServerAddress].refs != 1 {
		t.Fatalf("acquire should be recorded")
	}
}

func TestGrpcConnectionPool_Acquire_existingConnection(t *testing.T) {
	// given
	pool := NewGrpcConnectionPool(logger)
	defer pool.tearDown()

	// when
	connection1, _ := pool.Acquire(stubServerAddress, context.Background())
	connection2, _ := pool.Acquire(stubServerAddress, context.Background())

	// then
	if connection1 == nil || connection2 == nil {
		t.Fatalf("connection should be established")
	}
	if connection1 != connection2 {
		t.Fatalf("should reuse the same connection")
	}
	if pool.connections[stubServerAddress].refs != 2 {
		t.Fatalf("both acquires should be recorded")
	}
}

func TestGrpcConnectionPool_Release_close(t *testing.T) {
	// given
	pool := NewGrpcConnectionPool(logger)
	defer pool.tearDown()

	// when
	connection, _ := pool.Acquire(stubServerAddress, context.Background())
	// then
	if _, exists := pool.connections[stubServerAddress]; !exists {
		t.Fatalf("connection should exist after acquiring")
	}

	// when
	pool.Release(stubServerAddress)
	// then
	if _, exists := pool.connections[stubServerAddress]; exists {
		t.Fatalf("connection should be removed from the pool")
	}
	// this is a part of an experimental API, but it's ok for tests
	if connection.GetState() != connectivity.Shutdown {
		t.Fatalf("connection should be idle")
	}
}

func TestGrpcConnectionPool_Release_noop(t *testing.T) {
	// given
	pool := NewGrpcConnectionPool(logger)
	defer pool.tearDown()

	// when
	pool.Release("foo")

	// then
	// no error should be thrown
}

func TestNewGrpcHealthProbe_serverAndService(t *testing.T) {
	// given
	target := "my-host:1234/my.package.MyService"
	expectedServer := "my-host:1234"
	expectedRequest := hv1.HealthCheckRequest{Service: "my.package.MyService"}
	pool := NewGrpcConnectionPool(logger)
	defer pool.tearDown()

	// when
	probe := NewGrpcHealthProbe(target, pool)

	// then
	if probe.server != expectedServer {
		t.Errorf("bad server %s from string %s", probe.server, target)
	}

	if probe.request == nil || *probe.request != expectedRequest {
		t.Errorf("bad request %v from string %s", probe.request, target)
	}
}

func TestNewGrpcHealthProbe_server(t *testing.T) {
	// given
	target := "my-host:1234"
	expectedServer := "my-host:1234"
	expectedRequest := hv1.HealthCheckRequest{}
	pool := NewGrpcConnectionPool(logger)
	defer pool.tearDown()

	// when
	probe := NewGrpcHealthProbe(target, pool)

	// then
	if probe.server != expectedServer {
		t.Errorf("bad server %s from string %s", probe.server, target)
	}

	if probe.request == nil || *probe.request != expectedRequest {
		t.Errorf("bad GrpcHealthProbe service %v from string %s", probe.request, target)
	}
}

func TestGrpcHealthProbe_Check_App_Serving(t *testing.T) {
	// given
	pool := NewGrpcConnectionPool(logger)
	defer pool.tearDown()
	probe := NewGrpcHealthProbe(stubServerAddress, pool)

	// when
	err := probe.Check(time.Second)

	// then
	if err != nil {
		t.Errorf("app should be healthy, but got error %v", err)
	}
}

func TestGrpcHealthProbe_Check_App_NotServing(t *testing.T) {
	// given
	pool := NewGrpcConnectionPool(logger)
	defer pool.tearDown()
	probe := NewGrpcHealthProbe("no-host-exist:12345", pool)

	// when
	err := probe.Check(time.Second)

	// then
	if err == nil {
		t.Errorf("app should be unhealthy")
	}
}

func TestGrpcHealthProbe_Check_Service_Serving(t *testing.T) {
	// given
	stubServer.SetServingStatus("foo", hv1.HealthCheckResponse_SERVING)
	pool := NewGrpcConnectionPool(logger)
	defer pool.tearDown()
	probe := NewGrpcHealthProbe(fmt.Sprintf("%s/foo", stubServerAddress), pool)

	// when
	err := probe.Check(time.Second)

	// then
	if err != nil {
		t.Errorf("service should be healthy, but got error %v", err)
	}
}

func TestGrpcHealthProbe_Check_Service_NotServing(t *testing.T) {
	// given
	stubServer.SetServingStatus("foo", hv1.HealthCheckResponse_NOT_SERVING)
	pool := NewGrpcConnectionPool(logger)
	defer pool.tearDown()
	probe := NewGrpcHealthProbe(fmt.Sprintf("%s/foo", stubServerAddress), pool)

	// when
	err := probe.Check(time.Second)

	// then
	if err != GrpcUnhealthyError {
		t.Fatalf("service should not be healthy")
	}
}

func TestGrpcHealthProbe_Check_Service_NotFound(t *testing.T) {
	// given
	pool := NewGrpcConnectionPool(logger)
	defer pool.tearDown()
	probe := NewGrpcHealthProbe(fmt.Sprintf("%s/no.such.service.exists", stubServerAddress), pool)

	// when
	err := probe.Check(time.Second)

	// then
	if err == nil {
		t.Fatalf("service should not be healthy")
	}
}

func TestGrpcHealthProbe_Check_Service_Timeout(t *testing.T) {
	// given
	stubServer.SetServingStatus("foo", hv1.HealthCheckResponse_SERVING)
	pool := NewGrpcConnectionPool(logger)
	defer pool.tearDown()
	probe := NewGrpcHealthProbe(fmt.Sprintf("%s/foo", stubServerAddress), pool)

	// when
	err := probe.Check(time.Nanosecond) // it's impossible to make a call in 1 cpu tick, must timeout

	// then
	if err == nil {
		t.Fatalf("service should not be healthy")
	}
}
