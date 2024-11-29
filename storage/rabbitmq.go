package storage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"net/url"
	"sync"
	"time"
)

type rabbitMQ struct {
	//queue
	mqUrl       *url.URL
	conn        *amqp.Connection
	ch          *amqp.Channel
	consumeQos  *consumeQos
	consumeChan *<-chan amqp.Delivery

	// necessary sql db
	sqlUrl *url.URL
	sqlDB

	sync.Mutex
	signal
}

type consumeQos struct {
	prefetchCount int
	prefetchSize  int
	global        bool
}

type signal struct {
	iStop             context.Context    // exit all goroutine
	terminationSignal context.CancelFunc // called in Close()
}

func makeRabbitMQ(mqUrl_, sqlUrl_ *url.URL) (PersistentStorageServer, error) {
	var err error

	r := new(rabbitMQ)
	r.iStop, r.terminationSignal = context.WithCancel(context.Background())

	//connect Queue
	r.mqUrl = mqUrl_
	if err = r.connectQueue(); err != nil {
		return nil, err
	}
	r.statusListen()

	// connect SQL DB
	r.sqlDB.sqlUrl = sqlUrl_
	if err = r.sqlDB.connectSqlDB(); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *rabbitMQ) connectQueue() (err error) {
	r.conn, err = amqp.Dial(r.mqUrl.String())
	if err != nil {
		return
	}
	r.ch, err = r.conn.Channel()
	if err != nil {
		return
	}
	err = r.ch.Confirm(false)
	if err != nil {
		return
	}

	r.setConsumeQos()

	getConsumeQos := r.consumeQos
	err = r.ch.Qos(getConsumeQos.prefetchCount, getConsumeQos.prefetchSize, getConsumeQos.global)
	if err != nil {
		return err
	}
	r.consumeChan = new(<-chan amqp.Delivery)
	*r.consumeChan, err = r.ch.Consume("magnetico",
		"storage",
		false,
		false,
		false,
		false,
		amqp.Table{})

	return
}

func (r *rabbitMQ) setConsumeQos() {
	r.consumeQos = &consumeQos{
		prefetchCount: 1,
		prefetchSize:  0,
		global:        true,
	}
}

// handle the conversion of magnetic links into persistent storage
func (r *rabbitMQ) handlerTorrent() {
	var err error
	var torrentInfo SimpleTorrentSummary

	for {
		select {
		case xp := <-*r.consumeChan:
			r.Lock()
			msgContent := xp.Body
			if msgContent == nil {
				time.Sleep(13 * time.Second)
				r.Unlock()
				continue
			}
			err = json.Unmarshal(msgContent, &torrentInfo)
			if err != nil {
				_ = xp.Nack(false, false)
				r.Unlock()
				continue
			}
			zap.L().Debug("storage",
				zap.String("info", "consume msg"),
				zap.ByteString("details", msgContent))
			infoHash, _ := hex.DecodeString(torrentInfo.InfoHash)
			err = r.sqlDB.AddNewTorrent(infoHash, torrentInfo.Name, torrentInfo.Files)
			if err != nil {
				switch {
				case errors.Is(err, InfoHashExistErr):
					_ = xp.Ack(false)
					zap.L().Debug("storage",
						zap.String("info", "InfoHashExist"))
					r.Unlock()
					continue
				case errors.Is(err, DoesTorrentExistErr):
					_ = xp.Nack(false, true)
					r.Unlock()
					continue
				case errors.Is(err, SqlTransactionBeginErr) || errors.Is(err, SqlTransactionCommitErr):
					_ = xp.Nack(false, true)
					r.Unlock()
					continue
				case errors.Is(err, InsertTorrentsErr) || errors.Is(err, InsertTorrentsFilesErr):
					_ = xp.Reject(false)
					zap.L().Error("storage",
						zap.String("error", err.Error()))
					r.Unlock()
					continue
				case errors.Is(err, SqlTransactionRateLimiting):
					_ = xp.Nack(false, true)
					zap.L().Error("storage",
						zap.String("info", err.Error()))
					r.Unlock()
					continue
				}
			}

			err = xp.Ack(false)
			if err != nil {
				zap.L().Error("storage",
					zap.String("info", "failed to respond to MQ queue acknowledgment message"),
					zap.Error(err))
			}
			r.Unlock()

		case <-r.iStop.Done():
			return
		}
	}
}

// Status listening
func (r *rabbitMQ) statusListen() {
	// Check the MQ service status
	go func(rq *rabbitMQ) {
		checkInterval := time.NewTicker(10 * time.Second).C
		for {
			select {
			case <-checkInterval:
				rq.Lock()

				if r.ch.IsClosed() || r.conn.IsClosed() {
					if err := r.connectQueue(); err != nil {
						zap.L().Warn("storage",
							zap.String("info", "automatic reconnection to MQ server failed"),
							zap.Error(err))
					} else {
						zap.L().Info("storage",
							zap.String("info", "successfully reconnected to Queue Server"))
					}
				} else {
					zap.L().Debug("storage",
						zap.String("info", "queue service is online"),
					)
				}

				rq.Unlock()
			case <-rq.iStop.Done():
				return
			}
		}
	}(r)

}

func (r *rabbitMQ) Engine() queueEngine {
	return RabbitMQ
}

func (r *rabbitMQ) HandlerTorrent() error {
	go r.handlerTorrent()
	return nil
}

func (r *rabbitMQ) Close() (err error) {
	r.terminationSignal()
	if err = r.ch.Close(); err != nil {
		return err
	}
	if err = r.conn.Close(); err != nil {
		return err
	}

	if err = r.sqlDB.Close(); err != nil {
		return err
	}

	return
}
