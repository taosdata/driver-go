package unified

import (
	"net"
	"strconv"
	"sync"

	tLog "github.com/taosdata/driver-go/v3/log"
)

type cluster struct {
	mu    sync.Mutex
	addrs []string
}

func newCluster(addrs []string) *cluster {
	copyAddrs := append([]string(nil), addrs...)
	return &cluster{addrs: copyAddrs}
}

func (c *cluster) addresses() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.addrs))
	copy(out, c.addrs)
	return out
}

func (c *cluster) addAddresses(addrs []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addrs = appendUnique(c.addrs, addrs)
}

type clusterRegistry struct {
	mu                sync.Mutex
	endpointToCluster map[string]*cluster
}

func newClusterRegistry() *clusterRegistry {
	return &clusterRegistry{
		endpointToCluster: make(map[string]*cluster),
	}
}

// globalClusterRegistry is process-wide and keyed only by host:port.
// Adapter HA assumes host:port uniquely identifies one adapter endpoint in the
// process network namespace; clients for different logical clusters must not
// reuse the same host:port unless they intentionally share discovered topology.
var globalClusterRegistry = newClusterRegistry()

func (r *clusterRegistry) expand(seeds []string) []string {
	valid := validUniqueHostPorts(seeds)
	if len(valid) == 0 {
		return append([]string(nil), seeds...)
	}

	r.mu.Lock()
	clusters := r.findClustersLocked(valid)
	if len(clusters) != 1 {
		r.mu.Unlock()
		if len(clusters) > 1 {
			tLog.Warnf(0, "adapter HA: seeds match multiple clusters, seeds: %v", valid)
		}
		return append([]string(nil), seeds...)
	}
	c := clusters[0]
	r.mu.Unlock()

	return unionHostPorts(seeds, c.addresses())
}

func (r *clusterRegistry) update(addrs []string) {
	valid := validUniqueHostPorts(addrs)
	if len(valid) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	clusters := r.findClustersLocked(valid)
	var c *cluster
	switch len(clusters) {
	case 0:
		c = newCluster(valid)
	case 1:
		c = clusters[0]
		c.addAddresses(valid)
	default:
		tLog.Warnf(0, "adapter HA: discovered addresses match multiple clusters, addrs: %v", valid)
		return
	}

	for i := 0; i < len(valid); i++ {
		r.endpointToCluster[valid[i]] = c
	}
}

func (r *clusterRegistry) findClustersLocked(addrs []string) []*cluster {
	seen := make(map[*cluster]struct{})
	clusters := make([]*cluster, 0, 1)
	for i := 0; i < len(addrs); i++ {
		c, ok := r.endpointToCluster[addrs[i]]
		if !ok || c == nil {
			continue
		}
		if _, exists := seen[c]; exists {
			continue
		}
		seen[c] = struct{}{}
		clusters = append(clusters, c)
	}
	return clusters
}

func validUniqueHostPorts(addrs []string) []string {
	seen := make(map[string]struct{}, len(addrs))
	out := make([]string, 0, len(addrs))
	for i := 0; i < len(addrs); i++ {
		addr := addrs[i]
		if !isValidHostPort(addr) {
			if addr != "" {
				tLog.Warnf(0, "adapter HA: invalid host:port ignored: %s", addr)
			}
			continue
		}
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}
	return out
}

func isValidHostPort(addr string) bool {
	host, portString, err := net.SplitHostPort(addr)
	if err != nil || host == "" || portString == "" {
		return false
	}
	port, err := strconv.Atoi(portString)
	return err == nil && port >= 1 && port <= 65535
}

func unionHostPorts(first []string, second []string) []string {
	return appendUnique(first, second)
}

func appendUnique(first []string, second []string) []string {
	seen := make(map[string]struct{}, len(first)+len(second))
	out := make([]string, 0, len(first)+len(second))
	for _, values := range [][]string{first, second} {
		for i := 0; i < len(values); i++ {
			addr := values[i]
			if _, ok := seen[addr]; ok {
				continue
			}
			seen[addr] = struct{}{}
			out = append(out, addr)
		}
	}
	return out
}
