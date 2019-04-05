# gorabbit

golang rabbit amqp client wrapper for reliable publisher and consumer.

# How to use

```    
    type message struct{
        body string
        header string
    }
    publisher = &Publisher{
        Pool: &Pool{
                Dial: func() (Conn, error) {
                    return amqp.DialConfig("localhost:5672", amqp.Config{
                        Heartbeat: 10 * time.Second,
                        Vhost:     "",
                    })
                },
            },
        Reliable:true,
        QPS:100,

    }

    publisher.Publish("your exchange", &message{ body : "test", header : "test"})
```

# Define your own exchange decralator
you could define your exchange type by assigning exchange declaration configuration
```    
    type message struct{
        body string
        header string
    }
    publisher = &Publisher{
        Pool: &Pool{
                Dial: func() (Conn, error) {
                    return amqp.DialConfig("localhost:5672", amqp.Config{
                        Heartbeat: 10 * time.Second,
                        Vhost:     "",
                    })
                },
            },            
        Reliable:true,
        QPS:100,
        ExchangeDecralator : func(channel *amqp.Channel, exchange string) error {
                err := channel.ExchangeDeclare(
                    exchange,            // name
                    amqp.ExchangeFanout, // type
                    true,                // durable
                    true,               // auto-deleted
                    false,               // internal
                    true,               // no-wait
                    nil,                 // arguments
                )
                if err != nil {
                    return err
                }
                return nil
        },
    }

    publisher.Publish("your exchange", &message{ body : "test", header : "test"})
```

# Define your own publisher handler
you can change message decoder and publishing behavior by setting PublishHandler delegator.
```    
    type message struct{
        body string
        header string
    }
    publisher = &Publisher{
        Pool: &Pool{
                Dial: func() (Conn, error) {
                    return amqp.DialConfig("localhost:5672", amqp.Config{
                        Heartbeat: 10 * time.Second,
                        Vhost:     "",
                    })
                },
            },            
        Reliable:true,
        QPS:100,
        PublishHandler : func(channel *amqp.Channel, exchange string, payload interface{}) error {
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
        },
    }

    publisher.Publish("your exchange", &message{ body : "test", header : "test"})
```