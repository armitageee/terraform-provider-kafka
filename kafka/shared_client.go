package kafka

import "sync"

// The SDKv2 and framework servers run muxed in one process. The SDKv2 server
// is configured first (see main.go) and shares its client, so the framework
// side (list resources) needs no copy of the provider-config parsing.
var sharedClient struct {
	sync.RWMutex
	c *LazyClient
}

func setSharedClient(c *LazyClient) {
	sharedClient.Lock()
	sharedClient.c = c
	sharedClient.Unlock()
}

func getSharedClient() *LazyClient {
	sharedClient.RLock()
	defer sharedClient.RUnlock()
	return sharedClient.c
}
