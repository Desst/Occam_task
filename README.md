Implementation of service which subscribes to different sources and calculates average price for every ticker passed in config using the streamed data. If any of the sources returns an error or data received becomes delayed (threshold) then service resubscribes to those sources in order to get proper working stream again.


The sources should implement:

type PriceStreamSubscriber interface {
	ID() string
	SubscribePriceStream(t Ticker) (<-chan TickerPrice, <-chan error)
}



.test file is a simple demonstration of a single source of BTC_USD
