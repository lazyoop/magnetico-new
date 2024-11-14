package rabbitmq_reconnect

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"sync/atomic"
	"time"
)

type Publishing amqp.Publishing

type Channel struct {
	*amqp.Channel
	closed int32
	mutex  *sync.Mutex

	canceled int32

	qos                      channelQos
	autoDeletedQueues        []channelQueue
	autoDeletedQueueBindings []queueBinding
	autoDeletedExchanges     []channelExchange
}

type channelQos struct {
	applied       bool
	prefetchCount int
	prefetchSize  int
	global        bool
}

type queueBinding struct {
	queueName    string
	exchangeName string

	key    string
	noWait bool

	args amqp.Table
}

type channelQueue struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
	passive    bool
	args       amqp.Table
}

type channelExchange struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
	passive    bool
	args       amqp.Table
}

func (ch *Channel) Close() error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()

	if ch.isClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.Channel.Close()
}

func (ch *Channel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.NotifyClose(c)
}

func (ch *Channel) NotifyFlow(c chan bool) chan bool {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.NotifyFlow(c)
}

func (ch *Channel) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.NotifyReturn(c)
}

func (ch *Channel) NotifyCancel(c chan string) chan string {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.NotifyCancel(c)
}

func (ch *Channel) NotifyConfirm(ack, nack chan uint64) (chan uint64, chan uint64) {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.NotifyConfirm(ack, nack)
}

func (ch *Channel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.NotifyPublish(confirm)
}

func (ch *Channel) Cancel(consumer string, noWait bool) error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()

	err := ch.Channel.Cancel(consumer, noWait)
	if err != nil {
		return err
	}

	atomic.StoreInt32(&ch.canceled, 1)

	return nil
}

func (ch *Channel) QueueInspect(name string) (amqp.Queue, error) {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.QueueInspect(name)
}

func (ch *Channel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()

	bindingRestored := false

	for _, q := range ch.autoDeletedQueues {
		if name == q.name {
			ch.autoDeletedQueueBindings = append(ch.autoDeletedQueueBindings, queueBinding{
				queueName:    name,
				exchangeName: exchange,
				key:          key,
				noWait:       noWait,
				args:         args,
			})

			bindingRestored = true

			break
		}
	}

	if !bindingRestored {
		for _, e := range ch.autoDeletedExchanges {
			if e.name == exchange {
				ch.autoDeletedQueueBindings = append(ch.autoDeletedQueueBindings, queueBinding{
					queueName:    name,
					exchangeName: exchange,
					key:          key,
					noWait:       noWait,
					args:         args,
				})

				break
			}
		}
	}

	return ch.Channel.QueueBind(name, key, exchange, noWait, args)
}

func (ch *Channel) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.QueueUnbind(name, key, exchange, args)
}

func (ch *Channel) QueuePurge(name string, noWait bool) (int, error) {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.QueuePurge(name, noWait)
}

func (ch *Channel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

func (ch *Channel) Reject(tag uint64, requeue bool) error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.Reject(tag, requeue)
}

func (ch *Channel) Nack(tag uint64, multiple bool, requeue bool) error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.Nack(tag, multiple, requeue)
}

func (ch *Channel) Ack(tag uint64, multiple bool) error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.Ack(tag, multiple)
}

func (ch *Channel) Recover(requeue bool) error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.Recover(requeue)
}

func (ch *Channel) Confirm(noWait bool) error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.Confirm(noWait)
}

func (ch *Channel) Flow(active bool) error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.Flow(active)
}

func (ch *Channel) TxRollback() error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.TxRollback()
}

func (ch *Channel) TxCommit() error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.TxCommit()
}

func (ch *Channel) Tx() error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.Tx()
}

func (ch *Channel) Get(queue string, autoAck bool) (msg amqp.Delivery, ok bool, err error) {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()

	return ch.Channel.Get(queue, autoAck)
}

func (ch *Channel) Publish(exchange, key string, mandatory, immediate bool, msg Publishing) error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.Publish(exchange, key, mandatory, immediate, amqp.Publishing(msg))
}

func (ch *Channel) ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.ExchangeUnbind(destination, key, source, noWait, args)
}

func (ch *Channel) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	defer ch.mutex.Unlock()
	ch.mutex.Lock()
	return ch.Channel.ExchangeBind(destination, key, source, noWait, args)
}

func (ch *Channel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()
	return ch.Channel.ExchangeDelete(name, ifUnused, noWait)
}

func (ch *Channel) ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	if autoDelete {
		ch.autoDeletedExchanges = append(ch.autoDeletedExchanges, channelExchange{
			name:       name,
			kind:       kind,
			durable:    durable,
			autoDelete: autoDelete,
			internal:   internal,
			noWait:     noWait,
			passive:    true,
			args:       args,
		})
	}

	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	return ch.Channel.ExchangeDeclarePassive(name, kind, durable, autoDelete, internal, noWait, args)
}

func (ch *Channel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	if autoDelete {
		ch.autoDeletedExchanges = append(ch.autoDeletedExchanges, channelExchange{
			name:       name,
			kind:       kind,
			durable:    durable,
			autoDelete: autoDelete,
			internal:   internal,
			noWait:     noWait,
			passive:    false,
			args:       args,
		})
	}

	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	return ch.Channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (ch *Channel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if autoDelete {
		ch.autoDeletedQueues = append(ch.autoDeletedQueues, channelQueue{
			name:       name,
			durable:    durable,
			autoDelete: autoDelete,
			exclusive:  exclusive,
			noWait:     noWait,
			passive:    true,
			args:       args,
		})
	}

	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	return ch.Channel.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
}

func (ch *Channel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if autoDelete {
		ch.autoDeletedQueues = append(ch.autoDeletedQueues, channelQueue{
			name:       name,
			durable:    durable,
			autoDelete: autoDelete,
			exclusive:  exclusive,
			noWait:     noWait,
			passive:    false,
			args:       args,
		})
	}

	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	return ch.Channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (ch *Channel) Qos(prefetchCount, prefetchSize int, global bool) error {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	ch.qos.applied = true
	ch.qos.prefetchCount = prefetchCount
	ch.qos.prefetchSize = prefetchSize
	ch.qos.global = global

	return ch.Channel.Qos(prefetchCount, prefetchSize, global)
}

func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			ch.mutex.Lock()
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			ch.mutex.Unlock()
			if err != nil {
				// debugf("consume failed due [%s]", err)
				//zap.S().Debugf("consume failed due [%s]", err)
				time.Sleep(ReconnectDelay)
				continue
			}

			for msg := range d {
				// msg.Acknowledger = ch <- Possible race condition

				deliveries <- msg
			}

			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(ReconnectDelay)

			if ch.isClosed() || ch.isCanceled() {
				close(deliveries)

				break
			}
		}
	}()

	return deliveries, nil
}

func (ch *Channel) isClosed() bool {
	return atomic.LoadInt32(&ch.closed) == 1
}

func (ch *Channel) isCanceled() bool {
	return atomic.LoadInt32(&ch.canceled) == 1
}
