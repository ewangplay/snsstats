package main

import (
	"github.com/bitly/go-nsq"
)

type NSQHandler struct {
	topic string
}

func NewNSQHandler(topic string) (*NSQHandler, error) {
	nsqHandler := &NSQHandler{}
	nsqHandler.topic = topic

	return nsqHandler, nil
}

func (this *NSQHandler) HandleMessage(m *nsq.Message) error {
	LOG_DEBUG("从NSQ队列中接收到消息: %v", string(m.Body))

	go func(topic string, data []byte) error {
		var err error

		switch topic {
		case "SMSStatus":
			//解析状态数据并更新数据库
			err = this.handleSMSStatus(data)
			if err != nil {
				LOG_ERROR("处理短信的递送状态失败。失败原因：%v", err)
				return err
			}
		case "SMSPrivateMsg":
			err = this.handleSMSPrivateMsg(data)
			if err != nil {
				LOG_ERROR("处理用户回复的短信失败。失败原因：%v", err)
				return err
			}
		case "WeiboPrivateMsg":
			err = this.handleWeiboPrivateMsg(data)
			if err != nil {
				LOG_ERROR("处理微博私信失败。失败原因：%v", err)
				return err
			}
		case "WeixinPrivateMsg":
			err = this.handleWeixinPrivateMsg(data)
			if err != nil {
				LOG_ERROR("处理微信私信失败。失败原因：%v", err)
				return err
			}
		default:
			LOG_WARN("不支持的NSQ订阅主题[%v]", topic)
		}

		return nil
	}(this.topic, m.Body)

	return nil
}
