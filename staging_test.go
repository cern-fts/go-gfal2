package gfal2

import (
	"syscall"
	"testing"
	"time"
)

func getContext(t *testing.T) *Context {
	context, err := NewContext()
	if err != nil {
		t.Fatal(err)
	}
	return context
}

func TestBringOnlineOk(t *testing.T) {
	context := getContext(t)
	token, err := context.BringOnline("mock://host/file?staging_time=0", 100, 100, false)
	if err != nil {
		t.Error(err)
	}
	if token == "" {
		t.Error("Expecting a token")
	}
	t.Log(token)
}

func TestBringOnlineError(t *testing.T) {
	context := getContext(t)
	_, err := context.BringOnline("mock://host/file?staging_errno=2", 100, 100, false)
	if err == nil {
		t.Fatal("Expecting an error")
	}
	if err.Code() != 2 {
		t.Error("Was expecting 2, got ", err.Code())
	}
}

func TestBringOnlineList(t *testing.T) {
	context := getContext(t)
	urls := []string{
		"mock://host/file",
		"mock://host/file?staging_errno=2",
	}
	_, errors := context.BringOnlineList(urls, 100, 100, false)
	if errors == nil {
		t.Fatal("Expecting an array of errors")
	}
	if errors[0] != nil {
		t.Error("Was expecting the first to succeed, got ", errors[0])
	}
	if errors[1] == nil {
		t.Fatal("Was expecting the second to fail")
	}
	if errors[1].Code() != 2 {
		t.Error("Was expecting 2, got ", errors[1].Code())
	}
}

func TestPollList(t *testing.T) {
	context := getContext(t)
	urls := []string{
		"mock://host/file?staging_time=2",
		"mock://host/file?staging_time=2&staging_errno=2",
	}
	token, errors := context.BringOnlineList(urls, 100, 100, true)
	if errors == nil {
		t.Fatal("Expecting an array of errors")
	}
	for _, error := range errors {
		if error == nil || error.Code() != syscall.EAGAIN {
			t.Fatal("Was expecting an EAGAIN, got ", error)
		}
	}
	time.Sleep(5 * time.Second)
	errors = context.BringOnlinePollList(urls, token)
	if errors == nil {
		t.Fatal("Expecting an array of errors")
	}
	if errors[0] != nil {
		t.Error("Was expecting the first to succeed, got ", errors[0])
	}
	if errors[1] == nil {
		t.Fatal("Was expecting the second to fail")
	}
	if errors[1].Code() != 2 {
		t.Error("Was expecting 2, got ", errors[1].Code())
	}
}
