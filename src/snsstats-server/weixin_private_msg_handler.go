package main

import (
	"code.google.com/p/go.crypto/ssh"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

/*
CREATE TABLE jzl_weixin_msg(
    id int(11) not null primary key auto_increment,
    cid int(4) not null default '0',
    account_id int(11) not null default '0',
    to_user_openid varchar(64) not null default '',
    from_user_openidid varchar(64) not null default '',
    create_time datetime not null default '0000-00-00 00:00:00',
    msg_type varchar(20) not null default '',
    content text not null default '',
    msg_id varchar(32) not null default '',
    pic_url varchar(255) not null default '',
    media_id varchar(128) not null default '',
    format varchar(20) not null default '',
    thumb_media_id varchar(128) not null default '',
    location_x double not null default '0.0',
    location_y double not null default '0.0',
    scale int not null default '0',
    label varchar(255) not null default '',
    title varchar(64) not null default '',
    description varchar(255) not null default '',
    url varchar(255) not null default '',
    event varchar(20) not null default '',
    event_key varchar(64) not null default '',
    ticket varchar(128) not null default ''
);

CREATE TABLE jzl_weixin_msg_user(
    id int(11) not null primary key auto_increment,
    cid int(4) not null default '0',
    account_id int(11) not null default '0',
    openid varchar(64) not null default '',
    name varchar(64) not null default '',
    remark varchar(64) not null default '',
    headimgurl varchar(255) not null default '',
    sex int(4) not null default '0',
    country varchar(20) not null default '',
    province varchar(20) not null default '',
    city varchar(20) not null default '',
    unionid varchar(64) not null default '',
    groupid int(4) not null default '0'
    last_msg varchar(255) not null default '',
    refresh_time datetime not null default '0000-00-00 00:00:00'
);
*/

const (
	ADD_WEIXIN_MSG_SQL           string = "INSERT INTO jzl_weixin_msg (cid, account_id, to_user_openid, from_user_openid, create_time, msg_type, content, msg_id, pic_url, media_id, format, thumb_media_id, location_x, location_y, scale, label, title, description, url, event, event_key, ticket) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	GET_WEIXIN_ACCESS_TOKEN_SQL  string = "SELECT access_token FROM jzl_weixin_account WHERE cid=? AND account_id=?"
	GET_WEIXIN_USER_SQL          string = "SELECT unread_count FROM jzl_weixin_msg_user WHERE cid=? AND account_id=? AND openid=?"
	UPDATE_WEIXIN_USER_SQL       string = "UPDATE jzl_weixin_msg_user SET last_msg = ?, refresh_time = ?, msg_type = ?, unread_count = ? WHERE cid = ? AND account_id = ? AND openid = ?"
	ADD_WEIXIN_USER_SQL          string = "INSERT INTO jzl_weixin_msg_user (cid, account_id, openid, name, remark, headimgurl, sex, country, province, city, unionid, groupid, last_msg, refresh_time, unread_count, msg_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	ADD_WEIXIN_EVENT_MESSAGE_SQL string = "INSERT INTO jzl_weixin_msg (cid,account_id,to_user_openid,from_user_openid,create_time,msg_type,event,event_key,content) VALUES (?,?,?,?,?,?,?,?,'')"
)

type NginxHostInfo struct {
	Host        string
	Port        string
	User        string
	Passwd      string
	WebRootPath string // nginx服务的根目录
}

type WeixinPrivateMsg struct {
	CustomerId   int
	AccountId    int
	ToUserName   string
	FromUserName string
	CreateTime   int64
	MsgType      string
	Content      string
	MsgId        string
	PicUrl       string
	MediaId      string
	Format       string
	ThumbMediaId string
	Location_X   float64
	Location_Y   float64
	Scale        int
	Label        string
	Title        string
	Description  string
	Url          string
	Event        string
	EventKey     string
	Ticket       string
}

func getMD5String(source string) string {
	t := md5.New()
	io.WriteString(t, source)
	return fmt.Sprintf("%x", t.Sum(nil))

}

func scpWeixinVoiceFileToRemoteHost(nginxInfo *NginxHostInfo, path, fileName, content, media_type string) error {
	// 拷贝文件到nginx指定服务器
	//fileName为要保存的.amr文件，保存之后将其转换为.mp3文件
	clientConfig := &ssh.ClientConfig{
		User: nginxInfo.User,
		Auth: []ssh.AuthMethod{
			ssh.Password(nginxInfo.Passwd),
		},
	}
	hostPort := fmt.Sprintf("%s:%s", nginxInfo.Host, nginxInfo.Port)
	client, err := ssh.Dial("tcp", hostPort, clientConfig)
	if err != nil {
		return fmt.Errorf("%s|%s", "failed to dail", err.Error())
	}
	// 创建目录，如果不存在的话 mkdir -p xxx
	mksession, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("%s|%s", "failed to create mkdsession", err.Error())
	}
	defer mksession.Close()

	mkdirCmd := fmt.Sprintf("mkdir -p %s", path)
	if err := mksession.Run(mkdirCmd); err != nil {
		return fmt.Errorf("%s|%s", "Failed to mkdir path", err.Error())
	}
	// 拷贝数据文件
	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("%s|%s", "failed to create session", err.Error())
	}
	defer session.Close()

	isDone := make(chan error, 1)
	go func() {
		write, err := session.StdinPipe()
		defer write.Close()

		isDone <- err
		fmt.Fprintln(write, "C0664", len(content), fileName)
		fmt.Fprint(write, content)
		fmt.Fprint(write, "\x00") // 传输以\x00结束
	}()

	scpCmd := fmt.Sprintf("scp -qrt %s", path)
	if err := session.Run(scpCmd); err != nil {
		return fmt.Errorf("%s|%s", "Failed to run scp", err.Error())
	}

	if err := <-isDone; err != nil {
		return fmt.Errorf("%s|%s", "Failed to run scp", err.Error())
	}

	if media_type == "voice" {
		//转换格式
		convertsession, err := client.NewSession()
		if err != nil {
			return fmt.Errorf("%s|%s", "failed to create convertsession", err.Error())
		}
		defer convertsession.Close()
		dstName := path + strings.Split(fileName, ".")[0] + ".mp3"
		fmt.Printf("dstName: ", dstName)
		convertCmd := fmt.Sprintf("ffmpeg -i %v %v", path+fileName, dstName)
		if err := convertsession.Run(convertCmd); err != nil {
			return fmt.Errorf("%s|%s", "Failed to convert file", err.Error())
		}
		//删除文件
		delsession, err := client.NewSession()
		if err != nil {
			return fmt.Errorf("%s|%s", "failed to create delsession", err.Error())
		}
		defer delsession.Close()
		rmCmd := fmt.Sprintf("rm %v", path+fileName)
		if err := delsession.Run(rmCmd); err != nil {
			return fmt.Errorf("%s|%s", "Failed to remove file", err.Error())
		}
	}
	return nil
}

func (this *NSQHandler) saveMediaFile(privateMsg *WeixinPrivateMsg) error {
	//针对媒体文件类型保存文件路径和文件名
	//filename用于保存文件服务器中最终的文件名
	//srcname仅当需要对文件格式进行转换时使用,为转换前的文件名
	//weixin_file_path为读取文件时保存的文件路径
	//filepath为写文件到服务器时的文件路径
	//读取文件格式：http://files.jiuzhilan.com:/voice/b58f8069839a744a2e4ad5af8910f7aa/6197160281016493551.mp3
	//写入文件格式：files.jiuzhilan.com:date/files/voice/b58f8069839a744a2e4ad5af8910f7aa/6197160281016493551.mp3
	var filename, filepath, srcname, weixin_file_path string
	protocol := "http://"
	switch privateMsg.MsgType {
	case "voice":
		filename = string(privateMsg.MsgId) + "." + "mp3"
		srcname = string(privateMsg.MsgId) + "." + "amr"
		weixin_file_path = "/voice/" + getMD5String(fmt.Sprintf("%d", privateMsg.CustomerId)+fmt.Sprintf("%d", privateMsg.AccountId)+privateMsg.MsgId) + "/"
		filepath = "/data/files/" + weixin_file_path

	case "shortvideo":
		filename = string(privateMsg.MsgId) + "." + "mp4"
		weixin_file_path = "/shortvideo/" + getMD5String(fmt.Sprintf("%d", privateMsg.CustomerId)+fmt.Sprintf("%d", privateMsg.AccountId)) + "/"
		filepath = "/data/files/" + weixin_file_path

	case "image":
		filename = string(privateMsg.MsgId) + "." + "jpg"
		weixin_file_path = "/image/" + getMD5String(fmt.Sprintf("%d", privateMsg.CustomerId)+fmt.Sprintf("%d", privateMsg.AccountId)) + "/"
		filepath = "/data/files/" + weixin_file_path

	case "music":
		filename = string(privateMsg.MsgId) + "." + "mp3"
		weixin_file_path = "/music/" + getMD5String(fmt.Sprintf("%d", privateMsg.CustomerId)+fmt.Sprintf("%d", privateMsg.AccountId)) + "/"
		filepath = "/data/files/" + weixin_file_path

	default:
		LOG_ERROR("类型错误：[%v]，无法保存", privateMsg.MsgType)
		return fmt.Errorf("类型错误：[%v]，无法保存", privateMsg.MsgType)
	}
	LOG_DEBUG("filename: %v", filename)
	LOG_DEBUG("filepath: %v", filepath)

	contentStr, err := this.getMediaFile(privateMsg)

	//保存对应的媒体文件到文件服务器中
	host, _ := g_config.Get("files.host")
	port, _ := g_config.Get("files.port")
	user, _ := g_config.Get("files.user")
	password, _ := g_config.Get("files.password")
	nginxinfo := &NginxHostInfo{
		Host:        host,
		Port:        port,
		User:        user,
		Passwd:      password,
		WebRootPath: "",
	}
	switch privateMsg.MsgType {
	case "voice":
		err = scpWeixinVoiceFileToRemoteHost(nginxinfo, filepath, srcname, contentStr, privateMsg.MsgType)
	case "shortvideo":
		fallthrough
	case "music":
		fallthrough
	case "image":
		err = scpWeixinVoiceFileToRemoteHost(nginxinfo, filepath, filename, contentStr, privateMsg.MsgType)
	default:
		LOG_ERROR("类型错误：[%v]，无法保存", privateMsg.MsgType)
		return fmt.Errorf("类型错误：[%v]，无法保存", privateMsg.MsgType)
	}
	if err != nil {
		LOG_ERROR("[filepath:%v] [srcname:%v] 文件服务器写入失败: %v", filepath, srcname, err)
		return err
	}
	//更新jzl_weixin_msg中content字段为对应的文件服务器地址,同时更新jzl_weixin_msg_user中
	err = this.saveWeixinPrivateMsg(privateMsg, protocol+nginxinfo.Host+":"+weixin_file_path+filename)
	if err != nil {
		LOG_ERROR("保存微信私信失败: %v", err)
		return err
	}

	return nil

}

func (this *NSQHandler) getMediaFile(privateMsg *WeixinPrivateMsg) (string, error) {
	//获取access_token
	access_token, err := this.getWeixinAccessToken(privateMsg.CustomerId, privateMsg.AccountId)
	if err != nil {
		return "", err
	}
	var weixin_sender_client *WeixinSenderClient
	//通过MsgId获取媒体文件
	content, err := weixin_sender_client.DownloadTempMedia(access_token, privateMsg.MediaId)
	contentStr := string(content)
	if err != nil {
		return "", err
	}
	return contentStr, nil
}
func (this *NSQHandler) handleWeixinPrivateMsg(data []byte) error {
	var outputStr string
	var err error

	privateMsg := &WeixinPrivateMsg{}
	err = json.Unmarshal(data, privateMsg)
	if err != nil {
		LOG_ERROR("解析消息体失败: %v", err)
		return err
	}

	switch privateMsg.MsgType {
	case "event":
		//保存类型为event的事件推送
		err = this.SaveWeixinEventMsg(privateMsg)
		if err != nil {
			LOG_ERROR("保存微信私信消息失败。")
			return err
		}
		LOG_INFO("保存推送类型为text的消息成功。")
	case "shortvideo":
		err = this.saveMediaFile(privateMsg)
		if err != nil {
			LOG_ERROR("保存视频文件失败: %v", err)
			return err
		}
		LOG_INFO("保存视频文件成功。")
	case "voice":
		err = this.saveMediaFile(privateMsg)
		if err != nil {
			LOG_ERROR("保存语音文件失败: %v", err)
			return err
		}
		LOG_INFO("保存语音文件成功。")
	case "image":
		err = this.saveMediaFile(privateMsg)
		if err != nil {
			LOG_ERROR("保存图片私信失败: %v", err)
			return err
		}
		LOG_INFO("保存图片文件成功。")
	case "text":
		//解析出来的微信私信入数据库
		err = this.saveWeixinPrivateMsg(privateMsg, "")
		if err != nil {
			LOG_ERROR("保存微信私信失败: %v", err)
			return err
		}
		LOG_INFO("保存文本成功。")
	default:
		outputStr = fmt.Sprintf("类型为[%v]的私信被过滤掉", privateMsg.MsgType)
		LOG_ERROR(outputStr)
		return fmt.Errorf(outputStr)
	}

	LOG_INFO("保存类型为[%v]的微信私信成功. 私信内容: %v", privateMsg.MsgType, privateMsg)
	return nil
}

func (this *NSQHandler) saveWeixinPrivateMsg(msg *WeixinPrivateMsg, filepathandname string) (err error) {
	//格式化微信私信的创建时间
	create_time := time.Unix(msg.CreateTime, 0).Format("2006-01-02 15:04:05")
	var content string

	switch msg.MsgType {
	case "image":
		fallthrough
	case "shortvideo":
		fallthrough
	case "music":
		fallthrough
	case "voice":
		content = filepathandname
	default:
		content = msg.Content
	}
	fmt.Println("msg type, content, PicUrl: ", msg.MsgType, content, msg.PicUrl)
	//根据sender_id获取发送私信的用户信息
	err = this.saveWeixinUser(msg.CustomerId, msg.AccountId, msg.FromUserName, content, msg.MsgType)
	if err != nil {
		return err
	}

	//把微信私信保存到Mysql数据库中
	err = g_mysqladaptor.ExecFormat(ADD_WEIXIN_MSG_SQL, msg.CustomerId, msg.AccountId, msg.ToUserName,
		msg.FromUserName, create_time, msg.MsgType, content, msg.MsgId, content, msg.MediaId,
		msg.Format, msg.ThumbMediaId, msg.Location_X, msg.Location_Y, msg.Scale, msg.Label,
		msg.Title, msg.Description, msg.Url, msg.Event, msg.EventKey, msg.Ticket)
	if err != nil {
		LOG_ERROR("保存微信私信[%v]到数据库失败。失败原因：%v", content, err)
		return err
	}

	LOG_INFO("保存微信私信[%v]到数据库成功。", msg.Content)

	return
}

func (this *NSQHandler) getWeixinAccessToken(customer_id int, account_id int) (string, error) {
	row, err := g_mysqladaptor.QueryFormat(GET_WEIXIN_ACCESS_TOKEN_SQL, customer_id, account_id)
	if err != nil {
		return "", err
	}
	defer row.Close()

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
    "subscribe": 1,
    "openid": "o6_bmjrPTlm6_2sgVt7hMZOPfL2M",
    "nickname": "Band",
    "sex": 1,
    "language": "zh_CN",
    "city": "广州",
    "province": "广东",
    "country": "中国",
    "headimgurl":    "http://wx.qlogo.cn/mmopen/g3MonUZtNHkdmzicIlibx6iaFqAc56vxLSUfpb6n5WKSYVY0ChQKkiaJSgQ1dZuTOgvLLrhJbERQQ4eMsv84eavHiaiceqxibJxCfHe/0",
    "subscribe_time": 1382694957,
    "unionid": " o6_bmasdasdsad6_2sgVt7hMZOPfL"
    "remark": "",
    "groupid": 0
}
*/
func (this *NSQHandler) saveWeixinUser(customer_id int, account_id int, openid string, content string, msg_type string) (err error) {
	//首次到jzl_weixin_msg_user数据库表中查询该微信用户是否已经存在
	fmt.Println("cid,account_id,openid:", customer_id, account_id, openid)
	row, err := g_mysqladaptor.QueryFormat(GET_WEIXIN_USER_SQL, customer_id, account_id, openid)
	if err != nil {
		return err
	}
	defer row.Close()

	now := time.Now().Format("2006-01-02 15:04:05")
	if row.Next() {
		//找到了
		//读取unread_count字段，更新记录的刷新时间，并将unread_count字段+1
		var unread_count int64
		err = row.Scan(&unread_count)
		if err != nil {
			return err
		}
		fmt.Println("unread_count", unread_count)
		unread_count++
		fmt.Println("content, now, unread_count, msg_type, customer_id, account_id, openid:", content, now, unread_count, msg_type, customer_id, account_id, openid)
		err = g_mysqladaptor.ExecFormat(UPDATE_WEIXIN_USER_SQL, content, now, msg_type, unread_count, customer_id, account_id, openid)
		if err != nil {
			return err
		}
	} else {
		//没有找到

		//获取微信的access_token
		access_token, err := this.getWeixinAccessToken(customer_id, account_id)
		if err != nil {
			return err
		}

		result, err := g_weixinSenderClient.GetUserInfo(access_token, openid)
		if err != nil {
			return err
		}

		var info map[string]interface{}
		err = json.Unmarshal([]byte(result), &info)
		if err != nil {
			return err
		}

		name, _ := info["nickname"].(string)
		remark, _ := info["remark"].(string)
		headimgurl, _ := info["headimgurl"].(string)
		sex, _ := info["sex"].(int)
		country, _ := info["country"].(string)
		province, _ := info["province"].(string)
		city, _ := info["city"].(string)
		unionid, _ := info["unionid"].(string)
		groupid, _ := info["groupid"].(int)

		//保存到微信用户数据库表中，并将unread_count字段置为1
		err = g_mysqladaptor.ExecFormat(ADD_WEIXIN_USER_SQL, customer_id, account_id, openid, name, remark, headimgurl, sex, country, province, city, unionid, groupid, content, now, 1, msg_type)
		if err != nil {
			LOG_ERROR("保存微信用户[%v]到数据库失败。失败原因：%v", name, err)
			return err
		}
	}

	return nil
}

func (this *NSQHandler) SaveWeixinEventMsg(msg *WeixinPrivateMsg) error {
	create_timeStr := time.Unix(msg.CreateTime, 0).Format("2006-01-02 15:04:05")
	LOG_INFO("msg.CustomerId, msg.AccountId, msg.ToUserName, msg.FromUserName, msg.CreateTime, msg.MsgType, msg.Event, msg.EventKey: %v,%v,%v,%v,%v,%v,%v,%v", msg.CustomerId, msg.AccountId, msg.ToUserName, msg.FromUserName, create_timeStr, msg.MsgType, msg.Event, msg.EventKey)
	err := g_mysqladaptor.ExecFormat(ADD_WEIXIN_EVENT_MESSAGE_SQL, msg.CustomerId, msg.AccountId, msg.ToUserName, msg.FromUserName, create_timeStr, msg.MsgType, msg.Event, msg.EventKey)
	if err != nil {
		LOG_ERROR("save to mysql error: %v", err)
		return err
	}
	return nil
}
