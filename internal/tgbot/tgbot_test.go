package tgbot_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/retry"
	"github.com/co-codin/USDT-Tracker/internal/tgbot"
	"github.com/co-codin/USDT-Tracker/internal/tgbot/tgtest"
)

var fast = retry.Backoff{Initial: time.Millisecond, Max: time.Millisecond}

func TestSendMessageRetries429AndForbidden(t *testing.T) {
	srv := tgtest.New("123:SECRET")
	defer srv.Close()
	c, err := tgbot.New(tgbot.Options{Token: "123:SECRET", APIBase: srv.URL, Backoff: fast})
	if err != nil {
		t.Fatal(err)
	}
	srv.Fail429(1)
	start := time.Now()
	if err := c.SendMessage(context.Background(), "@chan", "<b>hi</b>", tgbot.SendOptions{HTML: true}); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 900*time.Millisecond {
		t.Fatal("retry_after not honoured")
	}
	if s := srv.Sent(); len(s) != 1 || s[0].ChatID != "@chan" || s[0].ParseMode != "HTML" {
		t.Fatalf("%+v", s)
	}
	srv.Forbid("42")
	err = c.SendMessage(context.Background(), "42", "x", tgbot.SendOptions{})
	if !tgbot.IsForbidden(err) {
		t.Fatalf("err = %v", err)
	}
}

func TestTokenNeverInErrors(t *testing.T) {
	srv := tgtest.New("right")
	defer srv.Close()
	c, _ := tgbot.New(tgbot.Options{Token: "WRONGSECRET", APIBase: srv.URL, Backoff: fast})
	err := c.SendMessage(context.Background(), "1", "x", tgbot.SendOptions{})
	if err == nil || strings.Contains(err.Error(), "WRONGSECRET") {
		t.Fatalf("err = %v", err)
	}
	c, _ = tgbot.New(tgbot.Options{Token: "WRONGSECRET", APIBase: "http://127.0.0.1:1", MaxRetries: 1, Backoff: fast})
	err = c.SendMessage(context.Background(), "1", "x", tgbot.SendOptions{})
	if err == nil || strings.Contains(err.Error(), "WRONGSECRET") {
		t.Fatalf("network err leaks token: %v", err)
	}
	if _, err := tgbot.New(tgbot.Options{}); err == nil {
		t.Fatal("empty token accepted")
	}
}

func TestGetUpdates(t *testing.T) {
	srv := tgtest.New("t")
	defer srv.Close()
	c, _ := tgbot.New(tgbot.Options{Token: "t", APIBase: srv.URL, Backoff: fast})
	srv.UserSays(7, "/start")
	srv.UserSays(8, "/help")
	ups, err := c.GetUpdates(context.Background(), 0, time.Second)
	if err != nil || len(ups) != 2 || ups[0].Message.Chat.ID != 7 || ups[1].Message.Text != "/help" {
		t.Fatalf("%+v %v", ups, err)
	}
	ups, err = c.GetUpdates(context.Background(), ups[1].UpdateID+1, time.Second)
	if err != nil || len(ups) != 0 {
		t.Fatalf("%+v %v", ups, err)
	}
}
