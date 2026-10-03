package kafka

import (
	"net"
	"sort"
	"strconv"

	"github.com/IBM/sarama"
)

type BrokerInfo struct {
	ID   int32
	Host string
	Port int
	Rack string
}

type ClusterInfo struct {
	ClusterID    string
	ControllerID int32
	Brokers      []BrokerInfo
}

// DescribeCluster returns the cluster ID, the active controller and the
// brokers as advertised to clients, from fresh metadata.
func (c *Client) DescribeCluster() (*ClusterInfo, error) {
	if err := c.client.RefreshMetadata(); err != nil {
		return nil, err
	}
	controller, err := c.client.Controller()
	if err != nil {
		return nil, err
	}
	// An empty topic list asks for cluster metadata only.
	md, err := controller.GetMetadata(sarama.NewMetadataRequest(c.kafkaConfig.Version, []string{}))
	if err != nil {
		return nil, err
	}
	info := &ClusterInfo{ControllerID: controller.ID()}
	if md.ClusterID != nil {
		info.ClusterID = *md.ClusterID
	}
	for _, b := range c.client.Brokers() {
		host, portStr, err := net.SplitHostPort(b.Addr())
		if err != nil {
			return nil, err
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, err
		}
		info.Brokers = append(info.Brokers, BrokerInfo{ID: b.ID(), Host: host, Port: port, Rack: b.Rack()})
	}
	sort.Slice(info.Brokers, func(i, j int) bool { return info.Brokers[i].ID < info.Brokers[j].ID })
	return info, nil
}
