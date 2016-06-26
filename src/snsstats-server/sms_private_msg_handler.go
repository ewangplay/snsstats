package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

const (
	ADD_SMS_MSG_SQL string = "INSERT INTO jzl_sms_msg (cid, mobile, content, service_no, receive_time) VALUES (?, ?, ?, ?, ?)"
)

/*
* Json结构数据，如下：
[
    {
        “mobile”: “1340114232”,
        “content”: “你好,我不需要”,
        “receivetime”: “2008-12-24 23:58:24”,
        “serviceno”: “10655020XXXXXXX”,
    },
    {
        “mobile”: “1380114786”,
        “content”: “本次活动的路线是什么?”,
        “receivetime”: “2008-12-24 23:58:24”,
        “serviceno”: “10655020XXXXXXX”,
    }
]
*/

type SMSPrivateMsg struct {
	Mobile      string
	Content     string
	Serviceno   string
	Receivetime string
}

func (this *NSQHandler) handleSMSPrivateMsg(data []byte) (err error) {
	var msgs []*SMSPrivateMsg
	err = json.Unmarshal(data, &msgs)
	if err != nil {
		LOG_ERROR("解析JSON数据[%v]失败。失败原因：%v", data, err)
		return
	}

	LOG_INFO("保存回复短信[%v]到数据库中", msgs)

	//解析出来的微博私信入数据库
	err = this.saveSMSPrivateMsg(msgs)
	if err != nil {
		LOG_ERROR("保存微博私信失败: %v", err)
		return err
	}

	LOG_INFO("保存用户回复短信到数据库成功")

	return
}

func (this *NSQHandler) saveSMSPrivateMsg(msgs []*SMSPrivateMsg) (err error) {
	for _, msg := range msgs {
		//TODO: 根据serviceno获取对应cid
		var cid int64

		//如果短信内容为TD，则进行退订流程
		if strings.Contains(strings.ToUpper(msg.Content), "TD") {
			LOG_INFO("bi[y=%v w=%v s=402]", 3, msg.Mobile)
			err = this.unsubcribeSMS(cid, msg.Mobile)
			if err != nil {
				LOG_ERROR("手机用户[%v]退订客户[%v]的短信失败，失败原因：%v", msg.Mobile, cid, err)
			}
		}

		//把微信私信保存到Mysql数据库中
		err = g_mysqladaptor.ExecFormat(ADD_SMS_MSG_SQL, cid, msg.Mobile, msg.Content, msg.Serviceno, msg.Receivetime)
		if err != nil {
			LOG_ERROR("保存短信私信[%v]到数据库失败。失败原因：%v", msg, err)
			return err
		}

		LOG_INFO("保存短信私信[%v]到数据库成功。", msg)
	}

	return
}

func (this *NSQHandler) unsubcribeSMS(cid int64, mobile string) error {
	var requestUrl string
	requestUrl = fmt.Sprintf(" http://api.jiuzhilan.com/v1/sms?cid=%v", cid)

	//Make Post Data
	reqBody, err := MakeUnsubcribeSMSBody(cid, mobile)
	if err != nil {
		LOG_ERROR("生成退订短信的POST数据失败. 失败原因：%v", err)
		return err
	}

	resp, err := http.Post(requestUrl, "text/plain", bytes.NewReader(reqBody))
	if err != nil {
		LOG_ERROR("请求退订手机[%v]失败. 失败原因：%v", mobile, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		LOG_ERROR("退订手机[%v]失败。Http状态码：%v", mobile, resp.StatusCode)
		return fmt.Errorf("%v", resp.StatusCode)
	}

	resBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		LOG_ERROR("读取HTTP响应信息失败. 失败原因：%v", err)
		return err
	}

	LOG_INFO("退订手机[%v]成功，返回状态：%v", mobile, string(resBody))

	return nil
}

/*
POST数据示例：
{
    "unsubcribe":["139112321","32132112233"],
    "customer_id":11
}
*/
func MakeUnsubcribeSMSBody(cid int64, mobile string) (body []byte, err error) {
	mBody := map[string]interface{}{
		"customer_id": cid,
		"unsubcribe":  []string{mobile},
	}
	return json.Marshal(mBody)
}
