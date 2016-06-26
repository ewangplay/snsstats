package main

import ()

type SNSStatsImpl struct {
}

func (this *SNSStatsImpl) Ping() (r string, err error) {
	LOG_INFO("请求ping方法")
	return "pong", nil
}
