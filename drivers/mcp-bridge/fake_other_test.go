//go:build !linux

package main

func isolationTool(string, map[string]string) (string, bool) { return "", false }
