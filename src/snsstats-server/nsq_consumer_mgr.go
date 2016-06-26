package main

import (
	"fmt"
	"time"
)

type NSQConsumerMgr struct {
	consumers map[string]*NSQConsumer
}

func NewNSQConsumerMgr() (*NSQConsumerMgr, error) {
	nsqConsumerMgr := &NSQConsumerMgr{}
	nsqConsumerMgr.consumers = make(map[string]*NSQConsumer)
	return nsqConsumerMgr, nil
}

func (this *NSQConsumerMgr) Init() error {
	var channel string
	var ok bool

	channel, ok = g_config.Get("nsq_service.channel")
	if !ok || channel == "" {
		channel = "online"
	}

	var topic, key string
	var handler *NSQHandler
	var consumer *NSQConsumer

	//SMSStatus Consumer
	topic = "SMSStatus"
	key = fmt.Sprintf("%v_%v", topic, channel)
	handler, _ = NewNSQHandler(topic)
	consumer, _ = NewNSQConsumer(topic, channel, handler)
	this.consumers[key] = consumer

	//SMSPrivateMsg Consumer
	topic = "SMSPrivateMsg"
	key = fmt.Sprintf("%v_%v", topic, channel)
	handler, _ = NewNSQHandler(topic)
	consumer, _ = NewNSQConsumer(topic, channel, handler)
	this.consumers[key] = consumer

	//WeiboPrivateMsg Consumer
	topic = "WeiboPrivateMsg"
	key = fmt.Sprintf("%v_%v", topic, channel)
	handler, _ = NewNSQHandler(topic)
	consumer, _ = NewNSQConsumer(topic, channel, handler)
	this.consumers[key] = consumer

	//WeixinPrivateMsg Consumer
	topic = "WeixinPrivateMsg"
	key = fmt.Sprintf("%v_%v", topic, channel)
	handler, _ = NewNSQHandler(topic)
	consumer, _ = NewNSQConsumer(topic, channel, handler)
	this.consumers[key] = consumer

	return nil
}

func (this *NSQConsumerMgr) Release() {
	for _, consumer := range this.consumers {
		if consumer.IsRunning() {
			consumer.Stop()
		}
	}
}

func (this *NSQConsumerMgr) Run() {
	go func() {
		for {
			for _, consumer := range this.consumers {
				if !consumer.IsRunning() {
					consumer.Start()
				}
			}

			//休息5分钟后再检查运行状态
			time.Sleep(30 * time.Second)
		}
	}()
}
