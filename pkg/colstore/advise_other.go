//go:build !unix

package colstore

func adviseRandom([]byte) {}

func adviseWillNeed([]byte, int, int) {}
