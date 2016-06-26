package main

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"jzlservice/weibosender"
)

type WeiboSenderClient struct {
}

func NewWeiboSenderClient() (*WeiboSenderClient, error) {
	return &WeiboSenderClient{}, nil
}

func (this *WeiboSenderClient) GetUserInfoById(access_token string, uid int64) (r string, err error) {
	var outputStr string
	var networkAddr string
	var addr, port string
	var addrIsSet, portIsSet bool

	LOG_INFO("获取用户[%v]的详细信息开始...", uid)

	addr, addrIsSet = g_config.Get("weibo_sender.addr")
	port, portIsSet = g_config.Get("weibo_sender.port")

	if addrIsSet && portIsSet {
		if addr != "" && port != "" {
			networkAddr = fmt.Sprintf("%s:%s", addr, port)
		} else {
			outputStr = "微博发送服务的网络地址设置错误"
			LOG_ERROR(outputStr)
			return "", fmt.Errorf(outputStr)
		}
	} else {
		outputStr = "微博发送服务的网络地址没有设置"
		LOG_ERROR(outputStr)
		return "", fmt.Errorf(outputStr)
	}

	trans, err := thrift.NewTSocket(networkAddr)
	if err != nil {
		LOG_ERROR("创建到微博发送服务[%v]的连接失败", networkAddr)
		return "", err
	}
	defer trans.Close()

	var protocolFactory thrift.TProtocolFactory
	protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()

	client := weibosender.NewWeiboSenderClientFactory(trans, protocolFactory)
	if err = trans.Open(); err != nil {
		LOG_ERROR("打开到微博发送服务WeiboSender的连接失败, 微博发送服务WeiboSender可能没有就绪，请检查服务状态")
		return "", err
	}

	r, err = client.GetUserInfoById(access_token, uid)
	if err != nil {
		LOG_ERROR("获取用户[%v]的详细信息失败. 失败原因：%v", uid, err)
		return "", err
	}

	LOG_INFO("获取用户[%v]的详细信息成功. 返回结果：%v", uid, r)

	return r, nil
}

func (this *WeiboSenderClient) GetUserInfoByName(access_token string, screen_name string) (r string, err error) {
	var outputStr string
	var networkAddr string
	var addr, port string
	var addrIsSet, portIsSet bool

	LOG_INFO("获取用户[%v]的详细信息开始...", screen_name)

	addr, addrIsSet = g_config.Get("weibo_sender.addr")
	port, portIsSet = g_config.Get("weibo_sender.port")

	if addrIsSet && portIsSet {
		if addr != "" && port != "" {
			networkAddr = fmt.Sprintf("%s:%s", addr, port)
		} else {
			outputStr = "微博发送服务的网络地址设置错误"
			LOG_ERROR(outputStr)
			return "", fmt.Errorf(outputStr)
		}
	} else {
		outputStr = "微博发送服务的网络地址没有设置"
		LOG_ERROR(outputStr)
		return "", fmt.Errorf(outputStr)
	}

	trans, err := thrift.NewTSocket(networkAddr)
	if err != nil {
		LOG_ERROR("创建到微博发送服务[%v]的连接失败，失败原因：%v", networkAddr, err)
		return "", err
	}
	defer trans.Close()

	var protocolFactory thrift.TProtocolFactory
	protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()

	client := weibosender.NewWeiboSenderClientFactory(trans, protocolFactory)
	if err = trans.Open(); err != nil {
		LOG_ERROR("打开到微博发送服务WeiboSender的连接失败, 微博发送服务WeiboSender可能没有就绪，请检查服务状态")
		return "", err
	}

	r, err = client.GetUserInfoByName(access_token, screen_name)
	if err != nil {
		LOG_ERROR("获取用户[%v]的详细信息失败. 失败原因：%s", screen_name, err)
		return "", err
	}

	LOG_INFO("获取用户[%v]的详细信息成功. 返回结果：%v", screen_name, r)

	return r, nil
}
