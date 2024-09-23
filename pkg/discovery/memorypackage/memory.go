package memory

import (
	"context"
	"errors"
	"sync"
	"time"

	"movieapp.com/pkg/discovery"
)

type serviceName string
type instanceID string

// Registry defines an in-memory service registry.
type Registry struct {
	sync.RWMutex
	serviceAddrs map[serviceName]map[instanceID]*serviceInstance
}
type serviceInstance struct {
	hostPort   string
	lastActive time.Time
}

// NewRegistry creates a new in-memory service
// registry instance.
func NewRegistry() *Registry {
	return &Registry{serviceAddrs: map[serviceName]map[instanceID]*serviceInstance{}}
}

// Register creates a service record in the registry.
func (r *Registry) Register(ctx context.Context, id instanceID, name serviceName, hostPort string) error {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.serviceAddrs[name]; !ok {
		r.serviceAddrs[name] = map[instanceID]*serviceInstance{}
	}
	r.serviceAddrs[name][id] = &serviceInstance{hostPort: hostPort,
		lastActive: time.Now()}
	return nil
}

// Deregister removes a service record from the
// registry.
func (r *Registry) Deregister(ctx context.Context, id instanceID, name serviceName) error {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.serviceAddrs[name]; !ok {
		return nil
	}
	delete(r.serviceAddrs[name], id)
	return nil
}

// ReportHealthyState is a push mechanism for
// reporting healthy state to the registry.
func (r *Registry) ReportHealthyState(id instanceID, name serviceName) error {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.serviceAddrs[name]; !ok {
		return errors.New("service is not registered yet")
	}
	if _, ok := r.serviceAddrs[name][id]; !ok {
		return errors.New("service instance is not registered yet")
	}
	r.serviceAddrs[name][id].lastActive = time.Now()
	return nil
}

// ServiceAddresses returns the list of addresses of
// active instances of the given service.
func (r *Registry) ServiceAddresses(ctx context.Context, name serviceName) ([]string, error) {
	r.RLock()
	defer r.RUnlock()
	if len(r.serviceAddrs[name]) == 0 {
		return nil, discovery.ErrNotFound
	}
	var res []string
	for _, i := range r.serviceAddrs[name] {
		if i.lastActive.Before(time.Now().Add(-5 * time.Second)) {
			continue
		}
		res = append(res, i.hostPort)
	}
	return res, nil
}
