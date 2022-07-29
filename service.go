package main

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Config struct {
	TickerSubs        []Ticker
	FairPriceInterval time.Duration
	ResubInterval     time.Duration
}

type Service struct {
	cfg           *Config
	sourceStreams []PriceStreamSubscriber

	wg              *sync.WaitGroup
	usm             *sync.Mutex
	unsubbedSources map[PriceStreamSubscriber][]Ticker

	tickersChan chan TickerPrice // data from all sources is streamed to this channel
	tickersData map[int64]map[Ticker]string
}

func NewService(cfg *Config, sourceStreams []PriceStreamSubscriber) *Service {
	us := make(map[PriceStreamSubscriber][]Ticker, len(sourceStreams))
	//assuming any sourcestream has data for every ticker
	for _, src := range sourceStreams {
		us[src] = cfg.TickerSubs
	}

	return &Service{
		cfg:             cfg,
		sourceStreams:   sourceStreams,
		wg:              &sync.WaitGroup{},
		usm:             &sync.Mutex{},
		unsubbedSources: us,

		tickersChan: make(chan TickerPrice),
		tickersData: make(map[int64]map[Ticker]string, cfg.FairPriceInterval.Milliseconds()),
	}
}

func (s *Service) Run(ctx context.Context) error {
	defer s.wg.Wait()
	s.subscribe(ctx)

	resubTicker := time.NewTicker(s.cfg.ResubInterval)
	calcTicker := time.NewTicker(s.cfg.FairPriceInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resubTicker.C:
			s.subscribe(ctx)
		case t := <-calcTicker.C:
			fairPrices := s.calcFairPrices(t)
			unixMilli := t.UnixMilli()
			log.Printf("Timestamp: %d\t%v\n", unixMilli, fairPrices)

			//clear old data
			for ts := range s.tickersData {
				if ts <= unixMilli {
					delete(s.tickersData, ts)
				}
			}
		case tp := <-s.tickersChan:
			//log.Printf("Quote received - %s: %s", tp.Ticker, tp.Price)
			tickerTs := tp.Time.UnixMilli()
			if _, ok := s.tickersData[tickerTs]; !ok {
				s.tickersData[tickerTs] = make(map[Ticker]string)
			}
			s.tickersData[tickerTs][tp.Ticker] = tp.Price
		}
	}

	return nil
}

func (s *Service) subscribe(ctx context.Context) {
	s.usm.Lock()
	defer s.usm.Unlock()

	if len(s.unsubbedSources) == 0 {
		return
	}

	for src, tickers := range s.unsubbedSources {
		for _, ticker := range tickers {
			s.wg.Add(1)
			go s.processTickerSub(ctx, ticker, src)
		}
	}

	//clear
	s.unsubbedSources = make(map[PriceStreamSubscriber][]Ticker)
}

func (s *Service) addPendingResub(ticker Ticker, src PriceStreamSubscriber) {
	s.usm.Lock()
	defer s.usm.Unlock()

	if _, ok := s.unsubbedSources[src]; !ok {
		s.unsubbedSources[src] = make([]Ticker, 0)
	}

	s.unsubbedSources[src] = append(s.unsubbedSources[src], ticker)
}

func (s *Service) processTickerSub(ctx context.Context, ticker Ticker, src PriceStreamSubscriber) {
	defer s.wg.Done()
	tpCh, eCh := src.SubscribePriceStream(ticker)
	log.Printf("Subscribed to %s for %s", src.ID(), ticker)

	for {
		select {
		case <-ctx.Done():
			log.Printf("StreamID: %s, Ticker: %s, Error: %v", src.ID(), ticker, ctx.Err())
			return
		case err := <-eCh:
			log.Printf("StreamID: %s, Ticker: %s, Error: %v", src.ID(), ticker, err)
			s.addPendingResub(ticker, src)
			return
		case tp, ok := <-tpCh:
			if ok {
				if err := s.processTickerPrice(tp); err != nil {
					log.Printf("StreamID: %s, Ticker: %s, Error: %v", src.ID(), ticker, err)
					s.addPendingResub(ticker, src)
					return
				}
			}
		}
	}
}

func (s *Service) processTickerPrice(tp TickerPrice) error {
	now := time.Now()
	if tp.Time.After(now) {
		return errors.New("invalid time") //quote from future
	}
	if now.Sub(tp.Time).Milliseconds() > s.cfg.FairPriceInterval.Milliseconds()+1 {
		return errors.New("stream is delayed")
	}

	s.tickersChan <- tp
	return nil
}

func (s *Service) calcFairPrices(t time.Time) map[Ticker]string {
	result := make(map[Ticker]string, len(s.cfg.TickerSubs))

	intervalPriceData := make(map[Ticker][]string, len(s.cfg.TickerSubs))
	tStart := t.Add(-s.cfg.FairPriceInterval).UnixMilli()
	tEnd := t.UnixMilli()

	for ts, tsData := range s.tickersData {
		if ts < tStart || ts > tEnd {
			continue
		}

		for ticker, priceStr := range tsData {
			if _, ok := intervalPriceData[ticker]; !ok {
				intervalPriceData[ticker] = make([]string, 0, tEnd-tStart)
			}
			intervalPriceData[ticker] = append(intervalPriceData[ticker], priceStr)
		}
	}

	for _, ticker := range s.cfg.TickerSubs {
		if data, ok := intervalPriceData[ticker]; !ok {
			result[ticker] = "-"
		} else {
			result[ticker] = calcFairPrice(data, 2)
		}
	}

	return result
}

//assuming price data has format like "123.4567" "-321.52" "20.0" etc.
//assuming "fair" = average and precision > 0
func calcFairPrice(priceData []string, precision int) string {
	var sumInt, sumFract int64

	count := 0
	for _, pStr := range priceData {
		parts := strings.Split(pStr, ".")
		if len(parts) != 2 { //invalid format
			continue
		}

		isNegative := false
		if r, _ := utf8.DecodeRuneInString(parts[0]); r == '-' { //decode first rune
			isNegative = true
		}

		intPart, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			continue
		}

		fractPart, err := parseFractionalPart(parts[1], precision)
		if err != nil {
			continue
		}

		if isNegative {
			fractPart *= -1
		}

		sumInt += intPart
		sumFract += fractPart
		count++
	}

	sumInt += sumFract / int64(math.Pow10(precision))
	sumFract = sumFract % int64(math.Pow10(precision))

	sumFloat, err := strconv.ParseFloat(fmt.Sprintf("%d.%0"+strconv.Itoa(precision)+"d", sumInt, sumFract), 64)
	if err != nil { // should not happen
		return ""
	}

	average := sumFloat / float64(count)
	return fmt.Sprintf("%."+strconv.Itoa(precision)+"f", average)
}

func parseFractionalPart(fractStr string, precision int) (int64, error) {
	if len(fractStr) == 0 {
		return 0, errors.New("empty string")
	}

	if precision == 1 {
		f, err := strconv.Atoi(fractStr[0:1])
		return int64(f), err
	}

	if len(fractStr) > precision {
		fractStr = fractStr[0:precision]
	}

	pow := precision - len(fractStr)

	count := len(fractStr)
	for i := 0; i < count; i++ {
		if strings.HasPrefix(fractStr, "0") { //skip leading zeroes
			fractStr = fractStr[1:]
			continue
		}

		frInt, err := strconv.ParseInt(fractStr, 10, 64)
		if err != nil {
			return 0, err
		}

		return frInt * int64(math.Pow10(pow)), nil
	}

	return 0, nil
}
