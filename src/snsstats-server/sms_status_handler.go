package main

import (
	"encoding/json"
	"strconv"
	"strings"
)

/*
* Json结构数据，如下：
[
    {
        “spnumber”: “1213”,
        “mobile”: “1340114232”,
        “status”: “DELIVRD”,
        “sendtime”: “2008-12-24 23:58:24”,
    },
    {
        “spnumber”: “1214”,
        “mobile”: “1380114564”,
        “status”: “-29”,
        “sendtime”: “2008-12-24 23:58:24”,
    }
]
*/
type SMSReportItem struct {
	Spnumber string
	Mobile   string
	Status   string
	Sendtime string
}

func (this *NSQHandler) handleSMSStatus(data []byte) error {
	var reports []*SMSReportItem
	err := json.Unmarshal(data, &reports)
	if err != nil {
		LOG_ERROR("解析JSON数据[%v]失败。失败原因：%v", data, err)
		return err
	}

	//根据Spnumber获取对应的customer_id, contact_id, etc...，然后输出BI日志
	for _, report := range reports {
		customer_id, contact_id, task_id, resource_id, group_id, err := this.getTaskInfo(report.Spnumber)
		if err != nil {
			LOG_ERROR("根据批次号[%v]获取任务信息失败，失败原因：%v", report.Spnumber, err)
			continue
		}

		if strings.Contains(report.Status, "DELIVRD") {
			LOG_INFO("bi[c=%v t=%v y=%v g=%v u=%v d=%v w=%v s=200 dt=%v]", customer_id, task_id, 3, group_id, contact_id, resource_id, report.Mobile, strings.Replace(report.Sendtime, " ", "_", -1))
			LOG_INFO("发送到[%v]的短信递送成功, 送达时间: %v", report.Mobile, report.Sendtime)
		} else {
			LOG_INFO("bi[c=%v t=%v y=%v g=%v u=%v d=%v w=%v s=303 dt=%v]", customer_id, task_id, 3, group_id, contact_id, resource_id, report.Mobile, strings.Replace(report.Sendtime, " ", "_", -1))
			LOG_ERROR("发送到[%v]的短信递送失败, 失败时间: %v", report.Mobile, report.Sendtime)
		}
	}

	return nil
}

func (this *NSQHandler) getTaskInfo(spnumber string) (customer_id, contact_id, task_id, resource_id, group_id int64, err error) {
	content, err := g_snsCache.Get(spnumber)
	if err != nil {
		return
	}

	//customer_id:contact_id:task_id:resource_id:group_id
	items := strings.Split(content, ":")
	customer_id, err = strconv.ParseInt(items[0], 10, 0)
	contact_id, err = strconv.ParseInt(items[1], 10, 0)
	task_id, err = strconv.ParseInt(items[2], 10, 0)
	resource_id, err = strconv.ParseInt(items[3], 10, 0)
	group_id, err = strconv.ParseInt(items[4], 10, 0)
	return
}
