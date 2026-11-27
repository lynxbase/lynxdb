package client

import (
	"context"
	"net/http"
	"net/url"
)

// PartitionInfo describes the parts of an index in [Start, End).
// Both boundaries are RFC 3339 timestamps in UTC.
type PartitionInfo struct {
	Start  string `json:"start"`
	End    string `json:"end"`
	Events int64  `json:"events"`
	Bytes  int64  `json:"bytes"`
	Parts  int    `json:"parts"`
}

func partitionsPath(index string) string {
	return "/indexes/" + url.PathEscape(index) + "/partitions"
}

// Partitions lists the partitions of index in event-time order.
func (c *Client) Partitions(ctx context.Context, index string) ([]PartitionInfo, error) {
	var resp struct {
		Partitions []PartitionInfo `json:"partitions"`
	}
	if _, err := c.doJSON(ctx, http.MethodGet, partitionsPath(index), nil, &resp); err != nil {
		return nil, err
	}
	return resp.Partitions, nil
}

// DropPartition permanently removes the partition beginning at start.
// Use the Start from Partitions or a YYYY-MM-DD date for midnight UTC.
func (c *Client) DropPartition(ctx context.Context, index, start string) (*PartitionInfo, error) {
	return c.changePartition(ctx, http.MethodDelete, partitionsPath(index)+"/"+url.PathEscape(start), nil)
}

// DetachPartition archives a partition to a new directory on the server
// and removes it from the index. The start has the same form as for DropPartition.
func (c *Client) DetachPartition(ctx context.Context, index, start, to string) (*PartitionInfo, error) {
	path := partitionsPath(index) + "/" + url.PathEscape(start) + "/detach"
	return c.changePartition(ctx, http.MethodPost, path, map[string]string{"to": to})
}

// AttachPartition imports a partition from a detached directory on the server.
func (c *Client) AttachPartition(ctx context.Context, index, from string) (*PartitionInfo, error) {
	return c.changePartition(ctx, http.MethodPost, partitionsPath(index)+"/attach", map[string]string{"from": from})
}

func (c *Client) changePartition(ctx context.Context, method, path string, body any) (*PartitionInfo, error) {
	var info PartitionInfo
	if _, err := c.doJSON(ctx, method, path, body, &info); err != nil {
		return nil, err
	}
	return &info, nil
}
