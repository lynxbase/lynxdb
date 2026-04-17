//go:build !debug

package memgov

func trackLease(*Lease) {}
