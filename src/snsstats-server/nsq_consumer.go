package main

import (
	"fmt"
	"github.com/bitly/go-nsq"
)

type NSQConsumer struct {
	consumer  *nsq.Consumer
	handler   nsq.Handler
	isRunning bool
	topic     string
	channel   string
}

func NewNSQConsumer(topic, channel string, handler nsq.Handler) (*NSQConsumer, error) {
	if topic == "" || channel == "" || handler == nil {
		return nil, fmt.Errorf("创建NSQConsumer对象失败，缺少参数")
	}

	nsqConsumer := &NSQConsumer{}
	nsqConsumer.consumer = nil
	nsqConsumer.isRunning = false
	nsqConsumer.topic = topic
	nsqConsumer.channel = channel
	nsqConsumer.handler = handler

	return nsqConsumer, nil
}

func (this *NSQConsumer) Start() error {
	go func(topic, channel string, handler nsq.Handler) error {
		var outputStr string
		var err error
		var nsqdNetAddr string
		var nsqlookupdNetAddr string
		var addr, port string
		var ok bool

		addr, ok = g_config.Get("nsq_service.nsqd.addr")
		if !ok || addr == "" {
			outputStr = "没有配置NSQD服务的网络IP"
			LOG_WARN(outputStr)
		}
		port, ok = g_config.Get("nsq_service.nsqd.port")
		if !ok || port == "" {
			outputStr = "没有配置NSQD服务的网络端口"
			LOG_WARN(outputStr)
		}
		if addr != "" && port != "" {
			nsqdNetAddr = fmt.Sprintf("%s:%s", addr, port)
		}

		addr, ok = g_config.Get("nsq_service.nsqlookupd.addr")
		if !ok || addr == "" {
			outputStr = "没有配置NSQLOOKUPD服务的网络IP"
			LOG_WARN(outputStr)
		}
		port, ok = g_config.Get("nsq_service.nsqlookupd.port")
		if !ok || port == "" {
			outputStr = "没有配置NSQLOOKUPD服务的网络端口"
			LOG_WARN(outputStr)
		}
		if addr != "" && port != "" {
			nsqlookupdNetAddr = fmt.Sprintf("%s:%s", addr, port)
		}

		//如果NSQD和NSWLOOKUPD服务的网络地址都没有配置，是不行的
		if nsqdNetAddr == "" && nsqlookupdNetAddr == "" {
			outputStr = "NSQD服务和NSQLOOKUPD服务的网络地址必须配置其中一个"
			LOG_ERROR(outputStr)
			return fmt.Errorf(outputStr)
		}

		cfg := nsq.NewConfig()
		cfg.MaxInFlight = 1000

		this.consumer, err = nsq.NewConsumer(topic, channel, cfg)
		if err != nil {
			LOG_ERROR("创建NSQ Consumer失败，失败原因：%v", err)
			return err
		}
		this.consumer.AddHandler(handler)

		if nsqlookupdNetAddr != "" {
			err = this.consumer.ConnectToNSQLookupd(nsqlookupdNetAddr)
			if err == nil {
				LOG_INFO("连接NSQLookupd服务[%v]成功", nsqlookupdNetAddr)

				this.isRunning = true
			} else {
				LOG_ERROR("连接NSQLookupd服务[%v]失败，失败原因：%v", nsqlookupdNetAddr, err)
			}
		}

		if !this.isRunning {
			if nsqdNetAddr != "" {
				err = this.consumer.ConnectToNSQD(nsqdNetAddr)
				if err == nil {
					LOG_INFO("连接NSQD服务[%v]成功", nsqdNetAddr)
					this.isRunning = true
				} else {
					LOG_ERROR("连接NSQD服务[%v]失败，失败原因：%v", nsqdNetAddr, err)
				}
			}
		}

		if this.isRunning {
			//等待当前Consumer终止
			<-this.consumer.StopChan

			LOG_INFO("NSQ Consumer [%v/%v] 退出", topic, channel)

			this.isRunning = false

			return nil
		} else {
			LOG_ERROR("NSQ Consumer [%v/%v] 创建失败", topic, channel)

			if this.consumer != nil {
				this.consumer.Stop()
				this.consumer = nil
			}

			return fmt.Errorf(outputStr)
		}

	}(this.topic, this.channel, this.handler)

	return nil
}

func (this *NSQConsumer) Stop() {
	if this.IsRunning() {
		if this.consumer != nil {
			this.consumer.Stop()
		}
	}
}

func (this *NSQConsumer) IsRunning() bool {
	return this.isRunning
}
