package main

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestAverage(t *testing.T) {
	input := []string{
		"123.456",
		"32.905",
		"-10.008",
		"1.4",
		"-0.05",
	}

	require.Equal(t, "29.541", calcFairPrice(input, 3))
	require.Equal(t, "29.5", calcFairPrice(input, 1))
	require.Equal(t, "29.5406", calcFairPrice(input, 4))
}

func TestService(t *testing.T) {
	pss := &MockExchangeStream{}
	service := NewService(testServiceCfg(), []PriceStreamSubscriber{pss})
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()

	err := service.Run(ctx)
	require.Equal(t, err, context.DeadlineExceeded)
}

func testServiceCfg() *Config {
	return &Config{
		TickerSubs:        []Ticker{BTCUSDTicker},
		FairPriceInterval: 1 * time.Second,
		ResubInterval:     10 * time.Second,
	}
}

type MockExchangeStream struct {
}

func (s *MockExchangeStream) ID() string {
	return "MockExchangeStream"
}

func (s *MockExchangeStream) SubscribePriceStream(t Ticker) (<-chan TickerPrice, <-chan error) {
	tpc := make(chan TickerPrice)
	ec := make(chan error)
	go func() {
		defer close(tpc)
		errTimer := time.NewTimer(7 * time.Second) // test resub
		for {
			select {
			case ts := <-time.After(500 * time.Millisecond):
				price := float64(21000+rand.Intn(500)) + rand.Float64()
				tpc <- TickerPrice{Ticker: t, Time: ts, Price: fmt.Sprintf("%.4f", price)}
			case <-errTimer.C:
				ec <- errors.New("timer fired")
				return
			}
		}
	}()
	return tpc, ec
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
