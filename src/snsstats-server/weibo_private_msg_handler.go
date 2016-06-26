package main

import (
	"encoding/json"
	"fmt"
	"time"
)

/*
CREATE TABLE jzl_weibo_msg(
    id int(11) not null primary key auto_increment,
    cid int(4) not null default '0',
    account_id varchar(20) not null default '',
    type varchar(16) not null default '',
    text varchar(255) not null default '',
    data varchar(255) not null default '',
    receiver_id varchar(20) not null default '',
    sender_id varchar(20) not null default '',
    created_time datetime not null default '0000-00-00 00:00:00'
);

CREATE TABLE jzl_weibo_user(
    id int(11) not null primary key auto_increment,
    cid int(4) not null default '0',
    account_id varchar(20) not null default '',
    user_id varchar(20) not null default '',
    name varchar(50) not null default '',
    screen_name varchar(50) not null default '',
    profile_image_url varchar(128) not null default '',
    location varchar(128) not null default '',
    description varchar(255) not null default '',
    url varchar(255) not null default ''
    last_msg varchar(255) not null default '',
    refresh_time datetime not null default '0000-00-00 00:00:00'
);
*/

const (
	ADD_WEIBO_MSG_SQL          string = "INSERT INTO jzl_weibo_msg (cid, account_id, type, text, data, receiver_id, sender_id, created_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	GET_WEIBO_ACCESS_TOKEN_SQL string = "SELECT access_token FROM jzl_weibo_account WHERE cid=? AND account_id=?"
	GET_WEIBO_USER_SQL         string = "SELECT unread_count, user_id FROM jzl_weibo_user WHERE cid=? AND account_id=? AND user_id=?"
	ADD_WEIBO_USER_SQL         string = "INSERT INTO jzl_weibo_user (cid, account_id, user_id, name, screen_name, profile_image_url, location, description, url, last_msg, refresh_time, unread_count, msg_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	UPDATE_WEIBO_USER_SQL      string = "UPDATE jzl_weibo_user SET last_msg = ?, refresh_time = ?, msg_type = ? WHERE cid = ? AND account_id = ? AND user_id = ? AND unread_count = ?"
)

/*
{
    "type": "Text",
    "Receiver_id": 2323231,
    "Sender_id": 3099232,
    "Created_at": “2008-12-24 23:58:24”,
    "Text": "你好，我想问一个问题。",
    "Data":{}
}
*/
type WeiboPrivateMsg struct {
	CustomerId  int
	AccountId   string
	Type        string
	Receiver_id int64
	Sender_id   int64
	Created_at  string
	Text        string
	Data        map[string]string
}

func (this *NSQHandler) handleWeiboPrivateMsg(data []byte) error {
	var outputStr string
	var err error

	privateMsg := &WeiboPrivateMsg{}
	err = json.Unmarshal(data, privateMsg)
	if err != nil {
		LOG_ERROR("解析消息体失败: %v", err)
		return err
	}

	if privateMsg.Type != "text" {
		LOG_WARN("类型为[%v]的私信被过滤掉", privateMsg.Type)
		return fmt.Errorf(outputStr)
	}

	//解析出来的微博私信入数据库
	err = this.saveWeiboPrivateMsg(privateMsg)
	if err != nil {
		LOG_ERROR("保存微博私信失败: %v", err)
		return err
	}

	LOG_INFO("保存微博私信成功. 私信内容: %v", privateMsg)

	return nil
}

func SerializeObject(obj interface{}) (string, error) {
	r, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}

	result := string(r)
	if result == "null" {
		return "", fmt.Errorf("null object")
	}

	return result, nil
}

func (this *NSQHandler) saveWeiboPrivateMsg(msg *WeiboPrivateMsg) (err error) {
	//对微博私信的附加数据进行序列化
	data, err := SerializeObject(msg.Data)
	if err != nil {
		return err
	}

	//格式化微博私信的创建时间
	t, err := time.Parse("Mon Jan 2 15:04:05 -0700 2006", msg.Created_at)
	if err != nil {
		return err
	}
	create_time := t.Format("2006-01-02 15:04:05")

	//根据sender_id获取发送私信的用户信息
	err = this.saveWeiboUser(msg.CustomerId, msg.AccountId, msg.Sender_id, msg.Text, msg.Type)
	if err != nil {
		return err
	}

	//把微博私信保存到Mysql数据库中
	err = g_mysqladaptor.ExecFormat(ADD_WEIBO_MSG_SQL, msg.CustomerId, msg.AccountId, msg.Type, msg.Text, data,
		msg.Receiver_id, msg.Sender_id, create_time)
	if err != nil {
		LOG_ERROR("保存微博私信[%v]到数据库失败。失败原因：%v", msg.Text, err)
		return err
	}

	LOG_INFO("保存微博私信[%v]到数据库成功。", msg.Text)

	return
}

func (this *NSQHandler) getWeiboAccessToken(customer_id int, account_id string) (string, error) {
	row, err := g_mysqladaptor.QueryFormat(GET_WEIBO_ACCESS_TOKEN_SQL, customer_id, account_id)
	if err != nil {
		return "", err
	}

	var access_token string
	if row.Next() {
		err = row.Scan(&access_token)
		if err != nil {
			return "", err
		}
	} else {
		//没有找到
		return "", fmt.Errorf("access token not found")
	}

	return access_token, nil
}

/*
{
    "id": 1404376560,
    "screen_name": "zaku",
    "name": "zaku",
    "province": "11",
    "city": "5",
    "location": "北京 朝阳区",
    "description": "人生五十年，乃如梦如幻；有生斯有死，壮士复何憾。",
    "url": "http://blog.sina.com.cn/zaku",
    "profile_image_url": "http://tp1.sinaimg.cn/1404376560/50/0/1",
    "domain": "zaku",
    "gender": "m",
    "followers_count": 1204,
    "friends_count": 447,
    "statuses_count": 2908,
    "favourites_count": 0,
    "created_at": "Fri Aug 28 00:00:00 +0800 2009",
    "following": false,
    "allow_all_act_msg": false,
    "geo_enabled": true,
    "verified": false,
    "status": {
        "created_at": "Tue May 24 18:04:53 +0800 2011",
        "id": 11142488790,
        "text": "我的相机到了。",
        "source": "<a href="http://weibo.com" rel="nofollow">新浪微博</a>",
        "favorited": false,
        "truncated": false,
        "in_reply_to_status_id": "",
        "in_reply_to_user_id": "",
        "in_reply_to_screen_name": "",
        "geo": null,
        "mid": "5610221544300749636",
        "annotations": [],
        "reposts_count": 5,
        "comments_count": 8
    },
    "allow_all_comment": true,
    "avatar_large": "http://tp1.sinaimg.cn/1404376560/180/0/1",
    "verified_reason": "",
    "follow_me": false,
    "online_status": 0,
    "bi_followers_count": 215
}
*/
func (this *NSQHandler) saveWeiboUser(customer_id int, account_id string, user_id int64, text, msg_type string) (err error) {
	//首次到jzl_weibo_user数据库表中查询该微博用户是否已经存在
	row, err := g_mysqladaptor.QueryFormat(GET_WEIBO_USER_SQL, customer_id, account_id, user_id)
	if err != nil {
		return err
	}

	now := time.Now().Format("2006-01-02 15:04:05")
	if row.Next() {
		//找到了
		//更新记录的刷新时间
		//将unread_count字段+1
		var unread_count int64
		var uidStr string
		err = row.Scan(&unread_count, &uidStr)
		if err != nil {
			return err
		}
		err = g_mysqladaptor.ExecFormat(UPDATE_WEIBO_USER_SQL, text, now, customer_id, account_id, user_id, unread_count+1, msg_type)
		if err != nil {
			return err
		}
	} else {
		//没有找到

		//获取微博的access_token
		access_token, err := this.getWeiboAccessToken(customer_id, account_id)
		if err != nil {
			return err
		}

		result, err := g_weiboSenderClient.GetUserInfoById(access_token, user_id)
		if err != nil {
			return err
		}

		var info map[string]interface{}
		err = json.Unmarshal([]byte(result), &info)
		if err != nil {
			return err
		}

		name, _ := info["name"].(string)
		screen_name, _ := info["screen_name"].(string)
		profile_image_url, _ := info["profile_image_url"].(string)
		location, _ := info["location"].(string)
		description, _ := info["description"].(string)
		url, _ := info["url"].(string)

		//保存到微博用户数据库表中
		//将unread_count字段置为1
		err = g_mysqladaptor.ExecFormat(ADD_WEIBO_USER_SQL, customer_id, account_id, user_id, name, screen_name, profile_image_url, location, description, url, text, now, 1, msg_type)
		if err != nil {
			LOG_ERROR("保存微博用户[%v]到数据库失败。失败原因：%v", name, err)
			return err
		}
	}

	return nil
}
