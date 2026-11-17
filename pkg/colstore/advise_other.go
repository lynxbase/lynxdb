//go:build !unix

package colstore

const pageSize = 4096

func adviseRandom([]byte) {}

func adviseWillNeed([]byte, int, int) {}

func adviseDontNeed([]byte) {}
