/* 
 * thrift interface for weixinsender
 */
namespace cpp jzlservice.weixinsender
namespace go jzlservice.weixinsender
namespace py jzlservice.weixinsender
namespace php jzlservice.weixinsender
namespace perl jzlservice.weixinsender
namespace java jzlservice.weixinsender

/**
* weixinsender service
*/
service WeixinSender {
    /**
    * @描述:
    *   服务的连通性测试
    * 
    * @返回: 
    *   返回pong表示服务正常; 返回空或其它标示服务异常
    */
	string ping(),		                

    /**
    * @描述:
    *   根据AppID和AppSecret获取AccessToken
    * 
    * @参数:
    *   appid: 第三方用户唯一凭证
    *   appsecret: 第三方用户唯一凭证密钥
    *
    * @返回: 
    *   返回该公众号调用接口的凭证AccessToken
    */
    string getAccessToken(1: string appid, 2: string appsecret),

    /**
    * @描述:
    *   添加客服账号
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   kfaccount: 客服账号
    *   nickname: 客服昵称
    *   password: 客服密码
    *
    * @返回: 
    *   状态信息，包括errcode, errmsg
    */
	string addKFAccount(1: string access_token, 2: string kfaccount, 3: string nickname, 4: string password),

    /**
    * @描述:
    *   修改客服账号
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   kfaccount: 客服账号
    *   nickname: 客服昵称
    *   password: 客服密码
    *
    * @返回: 
    *   状态信息，包括errcode, errmsg
    */
	string updateKFAccount(1: string access_token, 2: string kfaccount, 3: string nickname, 4: string password),
   
    /**
    * @描述:
    *   删除客服账号
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   kfaccount: 客服账号
    *   nickname: 客服昵称
    *   password: 客服密码
    *
    * @返回: 
    *   状态信息，包括errcode, errmsg
    */
	string deleteKFAccount(1: string access_token, 2: string kfaccount, 3: string nickname, 4: string password),

    /**
    * @描述:
    *   设置客服账号的头像
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   kfaccount: 客服账号
    *   media_file: 图片媒体文件
    *
    * @返回: 
    *   状态信息，包括errcode, errmsg
    */
    string setKFHeadImg(1: string access_token, 2: string kfaccount, 3: string media_file),

    /**
    * @描述:
    *   获取所有客服账号
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *
    * @返回: 
    *   返回所有客服账号的一个列表
    */
	string getKFAccountList(1: string access_token),
   
    /**
    * @描述:
    *   回复用户私信
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   touser: 发送用户的OpenID
    *   type: 发送消息的类型。text: 文本, image: 图片, voice: 语音, video: 视频, music: 音乐, news: 图文消息
    *   data: 发送消息的具体数据，不同类型的消息格式参考http://mp.weixin.qq.com/wiki/1/70a29afed17f56d537c833f89be979c9.html
    *   kfaccount: 以某个特定的客服来发送消息
    *
    * @返回: 
    *   状态信息，包括errcode, errmsg
    */
	string sendMessage(1: string access_token, 2: string touser, 3: string type, 4: string data, 5: string kfaccount),
 
    /**
    * @描述:
    *   上传临时媒体资源
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   type: 媒体资源的类型. image：图片，voice：语音，vedio：视频，thumb：缩略图
    *   media_file: 媒体文件
    *
    * @返回: 
    *   返回上传的状态，包括type, media_id, create_at
    */
    string uploadTempMedia(1: string access_token, 2: string type, 3: string media_file),
    
    /**
    * @描述:
    *   下载临时媒体资源
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   media_id: 媒体文件的ID
    *
    * @返回: 
    *   返回下载的媒体文件的二进制数据
    */
    binary downloadTempMedia(1: string access_token, 2: string media_id),

    /**
    * @描述:
    *   上传永久媒体资源
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   type: 媒体资源的类型. image：图片，voice：语音，vedio：视频，thumb：缩略图
    *   media_file: 媒体文件
    *
    * @返回: 
    *   返回上传的状态，包括type, media_id, create_at
    */
    string uploadPermanentMedia(1: string access_token, 2: string type, 3: string media_file),

    /**
    * @描述:
    *   下载永久媒体资源
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   media_id: 媒体文件的ID
    *
    * @返回: 
    *   返回下载的媒体文件的二进制数据
    */
    binary downloadPermanentMedia(1: string access_token, 2: string media_id),

    /**
    * @描述:
    *   删除永久媒体资源
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   media_id: 媒体文件的ID
    *
    * @返回: 
    *   返回删除的状态，包括errcode, errmsg
    */
    string deletePermanentMedia(1: string access_token, 2: string media_id),

    /**
    * @描述:
    *   上传图文消息
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   news: json格式的图文消息
    *
    * @返回: 
    *   返回上传的状态，包括type、media_id、create_at
    */
    string uploadNews(1: string access_token, 2: binary news),

    /**
    * @描述:
    *   发送图文消息
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   is_to_all: 是否发送给所有用户，true表示发送给所有用户，false表示按照下面设置的group_id发送
    *   group_id: 用户组ID，只有在is_to_all设置为false的时候才生效
    *   msg_type: 群发的消息类型，图文消息为mpnews，文本消息为text，语音为voice，音乐为music，图片为image，视频为video
    *   content: 要发送内容，如果是text类型的消息，就是具体的文本内容；其它类型则是对应的media_id
    *
    * @返回: 
    *   返回发送的状态，包括errcode, errmsg, msg_id
    */
    string sendNews(1: string access_token, 2: bool is_to_all, 3: string group_id, 4: string msg_type, 5: string content),

    /**
    * @描述:
    *   删除图文消息
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   msg_id: 已发送消息的ID
    *
    * @返回: 
    *   返回删除的状态，包括errcode, errmsg
    * @注意事项：
    *   1. 只有已经发送成功的消息才能删除
    *   2. 删除消息只是将消息的图文详情页失效，已经收到的用户，还是能在其本地看到消息卡片
    *   3. 删除群发消息只能删除图文消息和视频消息，其他类型的消息一经发送，无法删除
    */
    string deleteNews(1: string access_token, 2: string msg_id),

    /**
    * @描述:
    *   发给一个微信用户(OpenID)预览图文消息
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   touser: 发送用户的OpenID
    *   msg_type: 群发的消息类型，图文消息为mpnews，文本消息为text，语音为voice，音乐为music，图片为image，视频为video
    *   content: 要发送内容，如果是text类型的消息，就是具体的文本内容；其它类型则是对应的media_id
    *
    * @返回: 
    *   返回发送的状态，包括errcode, errmsg, msg_id
    */
    string previewNewsByOpenId(1: string access_token, 2: string touser, 3: string msg_type, 4: string content),

    /**
    * @描述:
    *   发给一个微信用户(微信号)预览图文消息
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   towxname: 发送用户的微信号
    *   msg_type: 群发的消息类型，图文消息为mpnews，文本消息为text，语音为voice，音乐为music，图片为image，视频为video
    *   content: 要发送内容，如果是text类型的消息，就是具体的文本内容；其它类型则是对应的media_id
    *
    * @返回: 
    *   返回发送的状态，包括errcode, errmsg, msg_id
    */
    string previewNewsByWeixinName(1: string access_token, 2: string towxname, 3: string msg_type, 4: string content),

    /**
    * @描述:
    *   获取图文消息的发送状态
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   msg_id: 已发送消息的ID
    *
    * @返回: 
    *   返回图文消息的发送状态，包括msg_id, msg_status
    */
    string getNewsStatus(1: string access_token, 2: string msg_id),

    /**
    * @描述:
    *   创建用户分组
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   group_name: 用户分组名称
    *
    * @返回: 
    *   返回创建用户分组的状态，包括group_name, group_id
    */
    string createUserGroup(1: string access_token, 2: string group_name),

    /**
    * @描述:
    *   更新用户分组
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   group_id: 已存在用户分组的ID
    *   new_group_name: 用户分组的新名称
    *
    * @返回: 
    *   返回更新用户分组的状态，包括errcode, errmsg
    */
    string updateUserGroup(1: string access_token, 2: string group_id, 3: string new_group_name),

    /**
    * @描述:
    *   获取用户分组列表
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *
    * @返回: 
    *   返回用户分组列表数据
    */
    string getUserGroupList(1: string access_token),

    /**
    * @描述:
    *   获取用户所在的分组
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   openid: 用户的OpenID
    *
    * @返回: 
    *   返回用户所在的分组
    */
    string getUserGroupByOpenID(1: string access_token, 2: string openid),

    /**
    * @描述:
    *   移动指定用户到指定的分组
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   openid_list: 要移动的用户列表
    *   to_groupid: 要移动的目标分组
    *
    * @返回: 
    *   返回移动用户的状态，包括errcode, errmsg
    */
    string moveUserToGroup(1: string access_token, 2: list<string> openid_list, 3: string to_groupid),

    /**
    * @描述:
    *   设置用户备注名
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   openid: 用户的OpenID
    *   remark: 备注名
    *
    * @返回: 
    *   返回移动用户的状态，包括errcode, errmsg
    */
    string remarkUser(1: string access_token, 2: string openid, 3: string remark),

    /**
    * @描述:
    *   获取用户的详细信息
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   openid: 用户的OpenID
    *
    * @返回: 
    *   返回用户的详细信息
    */
    string getUserInfo(1: string access_token, 2: string openid),

    /**
    * @描述:
    *   获取用户列表
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   next_openid: 拉取列表的后一个用户的OPENID
    *
    * @返回: 
    *   返回用户列表，如果一次获取不完，可以多次拉取
    */
    string getUserList(1: string access_token, 2: string next_openid),

    /**
    * @描述:
    *   创建菜单
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *   menu_data: json格式的菜单结构
    *
    * @返回: 
    *   返回创建菜单的状态，包括errcode, errmsg
    */
    string createMenu(1: string access_token, 2: binary menu_data),

    /**
    * @描述:
    *   删除菜单
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *
    * @返回: 
    *   返回删除菜单的状态，包括errcode, errmsg
    */
    string deleteMenu(1: string access_token),

    /**
    * @描述:
    *   获取菜单结构
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *
    * @返回: 
    *   返回菜单的json格式的结构
    */
    binary getMenu(1: string access_token),

    /**
    * @描述:
    *   获取图文群发总数据
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *
    * @返回: 
    *   返回json格式的图文统计数据
    */
    string getArticleTotal(1: string access_token, 2: string begin_date, 3: string end_date),

    /**
    * @描述:
    *   获取所有发送的图文统计数据
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *
    * @返回: 
    *   返回json格式的图文统计数据
    */
    string GetUserRead(1: string access_token, 2: string begin_date, 3: string end_date),

    /**
    * @描述:
    *   获取图文群发总数据
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *
    * @返回: 
    *   返回json格式的图文群发总数据
    */
    string GetArticleSummary(1: string access_token, 2: string begin_date, 3: string end_date),

    /**
    * @描述:
    *   获取图文群发总数据
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *
    * @返回: 
    *   返回json格式的图文群发总数据
    */
    string GetUserCumulate(1: string access_token, 2: string begin_date, 3: string end_date),

    /**
    * @描述:
    *   获取用户增减数据
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *
    * @返回: 
    *   返回json格式的用户增减数据
    */
    string GetUserSummary(1: string access_token, 2: string begin_date, 3: string end_date),

    /**
    * @描述:
    *   获取图文统计分时数据
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *
    * @返回: 
    *   返回json格式的图文统计分时数据
    */
    string GetUserReadHour(1: string access_token, 2: string begin_date, 3: string end_date),

    /**
    * @描述:
    *   获取图文分享转发数据
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *
    * @返回: 
    *   返回json格式的图文分享转发数据
    */
    string GetUserShare(1: string access_token, 2: string begin_date, 3: string end_date),

    /**
    * @描述:
    *   获取图文分享转发分时数据
    * 
    * @参数:
    *   access_token: 调用接口凭证
    *
    * @返回: 
    *   返回json格式的图文分享转发分时数据
    */
    string GetUserShareHour(1: string access_token, 2: string begin_date, 3: string end_date),
}

