package checks

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"log"
	"strings"
	"sync"
	"time"
)

var GrpcUnhealthyError = fmt.Errorf("gRPC application reported service unhealthy")

// GrpcConnectionPool holds connections to gRPC applications
// Single application may provide multiple services. To avoid creating
// connection per checked service, connections to applications are pooled.
// Note: gRPC connection is persistent. Once up, grpc implementation
// will maintain the connection and will do reconnects automatically.
//
// Connections are kept in a dictionary server -> connectionReference.
// Every time connectionReference is obtained, internal counter in the reference
// is incremented. Every time connection is released, counter is decremented.
// Once counter reaches zero, connection is closed.
type GrpcConnectionPool struct {
	mu          sync.Mutex
	connections map[string]*connectionReference
	logger      *log.Logger
}

func NewGrpcConnectionPool(logger *log.Logger) *GrpcConnectionPool {
	return &GrpcConnectionPool{
		connections: make(map[string]*connectionReference),
		logger:      logger,
	}
}

type connectionReference struct {
	connection *grpc.ClientConn
	refs       int
}

// Acquire connection to gRPC application.
// Returns existing connection to a gRPC application listening on specified
// server address if one exists. If connection does not exist, creates a new
// one.
// If connection attempt fails, returns with err != nil
func (p *GrpcConnectionPool) Acquire(server string, ctx context.Context) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	reference, exists := p.connections[server]
	if exists {
		reference.refs++
		p.logger.Printf("[DEBUG] grpc: acquired connection to '%s'. Total %d", server, reference.refs)
		return reference.connection, nil
	} else {
		conn, err := p.newConnection(server, ctx)
		if err != nil {
			p.logger.Printf("[DEBUG] grpc: connection attempt to '%s' failed", server)
			return nil, err
		}
		p.connections[server] = &connectionReference{
			connection: conn,
			refs:       1,
		}
		p.logger.Printf("[DEBUG] grpc: created connection to '%s'", server)
		return conn, nil
	}
}

// Release connection to gRPC application.
// Does nothing if no connection to app exists in pool.
// If this was the last usage of the connection, closes the connection.
func (p *GrpcConnectionPool) Release(server string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	reference, exists := p.connections[server]
	if !exists {
		p.logger.Printf("[WARN] grpc: attempted release of a connection to '%s' but it does not exist", server)
		return
	}
	reference.refs--
	p.logger.Printf("[DEBUG] grpc: released connection to '%s'. Total %d", server, reference.refs)
	if reference.refs <= 0 {
		err := reference.connection.Close()
		if err != nil {
			p.logger.Printf("[WARN] grpc: error while closing connection to '%s': %s", server, err.Error())
		}
		delete(p.connections, server)
		log.Printf("[DEBUG] grpc: removed connection to '%s' from pool", server)
	}
}

// tearDown this pool and close all managed connections.
// Used in testing code, should not be needed in prod code.
func (p *GrpcConnectionPool) tearDown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for server, ref := range p.connections {
		err := ref.connection.Close()
		if err != nil {
			p.logger.Printf("[WARN] grpc: error while closing connection to '%s': %s", server, err.Error())
		}
	}
	p.connections = make(map[string]*connectionReference)
}

func (p *GrpcConnectionPool) newConnection(server string, ctx context.Context) (conn *grpc.ClientConn, err error) {
	// TODO add configurable backoffs?
	// TODO add TLS
	conn, err = grpc.DialContext(ctx, server, grpc.WithInsecure())
	return
}

type GrpcHealthProbe struct {
	server  string
	request *healthpb.HealthCheckRequest
	client  healthpb.HealthClient
	pool    *GrpcConnectionPool
}

// NewGrpcHealthProbe constructs GrpcHealthProbe from target string in format
// server[/service]
// If service is omitted, health of the entire application is probed
func NewGrpcHealthProbe(target string, pool *GrpcConnectionPool) *GrpcHealthProbe {
	serverAndService := strings.SplitN(target, "/", 2)

	server := serverAndService[0]
	request := healthpb.HealthCheckRequest{}
	if len(serverAndService) > 1 {
		request.Service = serverAndService[1]
	}

	return &GrpcHealthProbe{
		server:  server,
		request: &request,
		pool:    pool,
	}
}

// Check if the target of this GrpcHealthProbe is healthy
// If nil is returned, target is healthy, otherwise target is not healthy
func (probe *GrpcHealthProbe) Check(timeout time.Duration) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err = probe.ensureAttached(ctx)
	if err != nil {
		return err
	}

	response, err := probe.client.Check(ctx, probe.request)
	if err != nil {
		return err
	}

	if response.Status != healthpb.HealthCheckResponse_SERVING {
		return GrpcUnhealthyError
	}

	return nil
}

func (probe *GrpcHealthProbe) ensureAttached(ctx context.Context) (err error) {
	if probe.client != nil {
		return
	}

	connection, err := probe.pool.Acquire(probe.server, ctx)
	if err != nil {
		return
	}

	probe.client = healthpb.NewHealthClient(connection)
	return nil
}

// Disconnect from the target server.
// Does nothing if already disconnected
func (probe *GrpcHealthProbe) Detach() {
	probe.client = nil
	probe.pool.Release(probe.server)
}
