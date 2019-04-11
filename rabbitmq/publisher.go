// Copyright 2019 Ichsan Risnandar
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package rabbitmq

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

type Publisher struct {
	Pool     *Pool
	QPS      int  // target query per second
	Reliable bool // use reliable publisher

	messages    chan *MQMessage
	initialized bool
	mu          sync.Mutex

	ExchangeDecralator func(channel *amqp.Channel, exchange string) error                      // exchange declaration function
	PublishHandler     func(channel *amqp.Channel, exchange string, payload interface{}) error // pulish handler function
}

type MQChannel struct {
	channel       *amqp.Channel
	sent          int64
	delivered     int64
	failed        int64
	confirmations chan amqp.Confirmation
}

type MQMessage struct {
	exchange string
	body     interface{}
}

func (m *Publisher) defaultPublishHandler() func(channel *amqp.Channel, exchange string, payload interface{}) error {
	return func(channel *amqp.Channel, exchange string, payload interface{}) error {
		body, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("Marshal Error: %s", err)
		}
		if err := channel.Publish(
			exchange, // publish to an exchange
			"",       // routing to 0 or more queues
			false,    // mandatory
			false,    // immediate
			amqp.Publishing{
				Body: body,
			},
		); err != nil {
			return fmt.Errorf("Exchange Publish: %s", err)
		}
		return nil
	}
}

func (m *Publisher) defaultExchangeDecralator() func(channel *amqp.Channel, exchange string) error {
	return func(channel *amqp.Channel, exchange string) error {
		err := channel.ExchangeDeclare(
			exchange,            // name
			amqp.ExchangeFanout, // type
			true,                // durable
			false,               // auto-deleted
			false,               // internal
			false,               // no-wait
			nil,                 // arguments
		)
		if err != nil {
			return err
		}
		return nil
	}
}

func (m *Publisher) load(number int) {
	payloads := make(map[string]*[]interface{})
	for i := 0; i < number; i++ {
		msg := <-m.messages
		msgs, ok := payloads[msg.exchange]
		if !ok {
			msgs = &[]interface{}{}
			payloads[msg.exchange] = msgs
		}
		*msgs = append(*msgs, msg.body)
	}

	for exchange, msgs := range payloads {
		errs := m.publishBatch(exchange, *msgs)
		if len(errs) > 0 {
			for _, err := range errs {
				log.Println("failed to publish update order", err)
			}
		}
	}
}

func (m *Publisher) start() {
	go func(module *Publisher) {
		scheduler := time.NewTicker((time.Second * 1))
		for {
			<-scheduler.C
			if len(module.messages) > 0 {
				if len(module.messages) > module.QPS {
					module.load(module.QPS)
				} else {
					module.load(len(module.messages))
				}
			}
		}
	}(m)
}

func (m *Publisher) getChannel(exchange string, conn Conn) (channel *MQChannel, err error) {
	channel = &MQChannel{}
	mqchannel, err := conn.Channel()
	if err != nil {
		return
	}

	channel.channel = mqchannel
	err = m.ExchangeDecralator(mqchannel, exchange)
	if err != nil {
		return nil, fmt.Errorf("Channel could not be declare: %s", err)
	}

	if m.Reliable {
		if err = channel.channel.Confirm(false); err != nil {
			return nil, fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		}
		channel.confirmations = channel.channel.NotifyPublish(make(chan amqp.Confirmation, m.QPS))
	}

	return
}

func (m *Publisher) Publish(exchange string, payload interface{}) {
	if m.initialized == false {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.initialized = true
		m.messages = make(chan *MQMessage, m.QPS*10)
		if m.PublishHandler == nil {
			m.PublishHandler = m.defaultPublishHandler()
		}

		if m.ExchangeDecralator == nil {
			m.ExchangeDecralator = m.defaultExchangeDecralator()
		}

		m.start()
	}
	m.messages <- &MQMessage{
		exchange: exchange,
		body:     payload,
	}
}

func (m *Publisher) publish(channel *MQChannel, exchange string, payload interface{}) (err error) {
	if err := m.PublishHandler(channel.channel, exchange, payload); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}
	atomic.StoreInt64(&channel.sent, atomic.AddInt64(&channel.sent, 1))
	return
}

func (m *Publisher) publishBatch(exchange string, payloads []interface{}) []error {
	errs := []error{}
	requeuAll := func() {
		for _, payload := range payloads {
			m.messages <- &MQMessage{
				exchange: exchange,
				body:     payload,
			}
		}
	}

	conn, err := m.Pool.Get()
	defer conn.Close()
	if err != nil {
		requeuAll()
		errs = append(errs, err)
		return errs
	}

	channel, err := m.getChannel(exchange, conn)
	if err != nil {
		requeuAll()
		errs = append(errs, err)
		return errs
	}

	for _, payload := range payloads {
		err := m.publish(channel, exchange, payload)
		if err != nil {
			m.messages <- &MQMessage{
				exchange: exchange,
				body:     payload,
			}
			errs = append(errs, err)
		}
	}

	if m.Reliable {
		for i := int64(0); i < channel.sent; i++ {
			confirm := <-channel.confirmations
			if !confirm.Ack {
				atomic.StoreInt64(&channel.failed, atomic.AddInt64(&channel.failed, 1))
			} else {
				atomic.StoreInt64(&channel.delivered, atomic.AddInt64(&channel.delivered, 1))
			}
		}
	}
	return errs
}
