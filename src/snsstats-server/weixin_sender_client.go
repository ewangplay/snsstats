package main

import (
	"encoding/json"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"jzlservice/weixinsender"
)

type WeixinSenderClient struct {
}

type WeixinErrorInfo struct {
	Errcode int64  `json:"errcode"`
	Errmsg  string `json:"errmsg"`
}

func NewWeixinSenderClient() (*WeixinSenderClient, error) {
	return &WeixinSenderClient{}, nil
}

func (this *WeixinSenderClient) GetUserInfo(access_token string, openid string) (r string, err error) {
	var outputStr string
	var networkAddr string
	var addr, port string
	var addrIsSet, portIsSet bool

	LOG_INFO("获取用户[%v]的详细信息开始.", openid)

	addr, addrIsSet = g_config.Get("weixin_sender.addr")
	port, portIsSet = g_config.Get("weixin_sender.port")

	if addrIsSet && portIsSet {
		if addr != "" && port != "" {
			networkAddr = fmt.Sprintf("%s:%s", addr, port)
		} else {
			outputStr = "WeixinSender服务的网络地址设置错误"
			LOG_ERROR(outputStr)
			return "", fmt.Errorf(outputStr)
		}
	} else {
		outputStr = "WeixinSender服务的网络地址没有设置"
		LOG_ERROR(outputStr)
		return "", fmt.Errorf(outputStr)
	}

	trans, err := thrift.NewTSocket(networkAddr)
	if err != nil {
		LOG_ERROR("创建到WeixinSender服务[%v]的连接失败", networkAddr)
		return "", err
	}
	defer trans.Close()

	var protocolFactory thrift.TProtocolFactory
	protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()

	client := weixinsender.NewWeixinSenderClientFactory(trans, protocolFactory)
	if err = trans.Open(); err != nil {
		LOG_ERROR("打开到WeixinSender服务的连接失败, WeixinSender服务可能没有就绪，请检查服务状态")
		return "", err
	}

	r, err = client.GetUserInfo(access_token, openid)
	if err != nil {
		LOG_ERROR("获取用户[%v]的详细信息失败，失败原因：%v", openid, err)
		return "", err
	}

	LOG_INFO("获取用户[%v]的详细信息成功", openid)

	return
}
func ParseErrMsg(result []byte) error {
	var info WeixinErrorInfo
	err := json.Unmarshal(result, &info)
	if err == nil {
		if info.Errcode != 0 {
			return fmt.Errorf("%v: %v", info.Errcode, info.Errmsg)
		}
	}
	return nil
}
func (this *WeixinSenderClient) DownloadTempMedia(access_token string, media_id string) (r []byte, err error) {
	var outputStr string
	var networkAddr string
	var addr, port string
	var addrIsSet, portIsSet bool

	LOG_INFO("下载临时媒体资源[%v]开始.", media_id)

	addr, addrIsSet = g_config.Get("weixin_sender.addr")
	port, portIsSet = g_config.Get("weixin_sender.port")

	if addrIsSet && portIsSet {
		if addr != "" && port != "" {
			networkAddr = fmt.Sprintf("%s:%s", addr, port)
		} else {
			outputStr = "WeixinSender服务的网络地址设置错误"
			LOG_ERROR(outputStr)
			return nil, fmt.Errorf(outputStr)
		}
	} else {
		outputStr = "WeixinSender服务的网络地址没有设置"
		LOG_ERROR(outputStr)
		return nil, fmt.Errorf(outputStr)
	}

	trans, err := thrift.NewTSocket(networkAddr)
	if err != nil {
		LOG_ERROR("创建到WeixinSender服务[%v]的连接失败", networkAddr)
		return nil, err
	}
	defer trans.Close()

	var protocolFactory thrift.TProtocolFactory
	protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()

	client := weixinsender.NewWeixinSenderClientFactory(trans, protocolFactory)
	if err = trans.Open(); err != nil {
		LOG_ERROR("打开到WeixinSender服务的连接失败, WeixinSender服务可能没有就绪，请检查服务状态")
		return nil, err
	}

	r, err = client.DownloadTempMedia(access_token, media_id)
	if err != nil {
		LOG_ERROR("下载临时媒体资源[%v]失败，失败原因：%v", media_id, err)
		return nil, err
	}

	LOG_INFO("下载临时媒体资源[%v]成功", media_id)

	return

}
