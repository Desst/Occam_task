package main

import "time"

type Ticker string

const (
	BTCUSDTicker Ticker = "BTC_USD"
)

type TickerPrice struct {
	Ticker Ticker
	Time   time.Time
	Price  string // decimal value. example: "0", "10", "12.2", "13.2345122"
}

type PriceStreamSubscriber interface {
	ID() string
	SubscribePriceStream(t Ticker) (<-chan TickerPrice, <-chan error)
}
