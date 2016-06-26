/* 
 * thrift interface for weibosender
 */
namespace cpp jzlservice.weibosender
namespace go jzlservice.weibosender
namespace py jzlservice.weibosender
namespace php jzlservice.weibosender
namespace perl jzlservice.weibosender
namespace java jzlservice.weibosender

/**
* struct WeiboStatus
* weibo status structure description.
*/
struct WeiboStatus {
    1: i64 task_id,             //任务ID

    2: string access_token = "",        //从OAuth认证的身份令牌

    3: string status = "",              //要发布的微博文本内容，必须做URLencode，内容不超过140个汉字

    4: i32 visible = 0,                 //微博的可见性，0：所有人能看，1：仅自己可见，2：密友可见，3：指定分组可见，默认为0

    5: string list_id = "",             //微博的保护投递指定分组ID，只有当visible参数为3时生效且必选

    6: double latitude,                 //纬度，有效范围：-90.0到+90.0，+表示北纬，默认为0.0

    7: double longitude,                //经度，有效范围：-180.0到+180.0，+表示东经，默认为0.0

    8: string annotations = "",         //或者多个元数据，必须以json字串的形式提交，字串长度不超过512个字符，具体内容可以自定

    9: string real_ip = "",             //开发者上报的操作用户真实IP，形如：211.156.0.1

    10: string pic = "",                //要上传的图片，仅支持JPEG、GIF、PNG格式，图片大小小于5M
}

/**
* weibosender service
*/
service WeiboSender {
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
    *   发送微博接口
    *
    * @参数:
    *   weibo_status: 要发送的微博状态
    *
    * @返回:
    *   发送状态, JSON格式数据，具体内容参考：http://open.weibo.com/wiki/2/statuses/update
    */
    string sendStatus(1: WeiboStatus weibo_status), 

    /**
    * @描述:
    *   发送私信接口
    *
    * @参数:
    *   access_token: 从OAuth认证的身份令牌
    *   type: 需要以何种类型的消息进行响应，text：纯文本、articles：图文、position：位置
    *   data: 消息数据，具体内容严格遵循type类型对应格式，必须为json做URLEncode后的字符串格式，采用UTF-8编码
    *   receiver_id: 消息接收方的ID
    *   save_sender_box: 取值为0或1，不填则默认为1。取值为1时，通过本接口发送的消息会进入发送方的私信箱；取值为0时，通过本接口发送的消息不会进入发送方的私信箱
    *
    * @返回:
    *   发送状态，JSON格式数据，具体内容参考: http://open.weibo.com/wiki/%E5%8F%91%E9%80%81%E5%AE%A2%E6%9C%8D%E6%B6%88%E6%81%AF
    */
    string sendMessage(1: string access_token, 2: string type, 3: string data, 4: i64 receiver_id, 5: i32 save_sender_box),

    /** 
    * @描述:
    *   根据用户ID获取用户信息
    *
    * @参数:
    *   access_token: 通过OAuth2认证得到的身份标识
    *   uid: 用户的id 
    *
    * @返回:
    *   请求成功返回用户的详细信息，JSON格式数据，具体内容参考: http://open.weibo.com/wiki/2/users/show
    *   请求失败返回空
    */
    string getUserInfoById(1: string access_token, 2: i64 uid), 

    /** 
    * @描述:
    *   根据用户昵称获取用户信息
    *
    * @参数:
    *   access_token: 通过OAuth2认证得到的身份标识
    *   screen_name: 用户的昵称
    *
    * @返回:
    *   请求成功返回用户的详细信息，JSON格式数据，具体内容参考: http://open.weibo.com/wiki/2/users/show
    *   请求失败返回空
    */
    string getUserInfoByName(1: string access_token, 2: string screen_name),

    /**
    * @描述:
    *   获取用户最新发布的微博
    * 
    * @参数:
    *   access_token: 通过OAuth2认证得到的身份标识
    *   since_id: 若指定此参数，则返回ID比since_id大的微博（即比since_id时间晚的微博），默认为0
    *   max_id: 若指定此参数，则返回ID小于或等于max_id的微博，默认为0
    *   count: 单页返回的记录条数，最大不超过100，超过100以100处理，默认为20
    *   page: 返回结果的页码，默认为1
    *
    * @返回:
    *   请求成功返回最新发布的微博列表, JSON格式数据，具体内容参考：http://open.weibo.com/wiki/2/statuses/user_timeline
    *   请求失败返回空
    */
    string getStatuses(1: string access_token, 2: i64 since_id, 3: i64 max_id, 4: i32 count, 5: i32 page), 

    /**
    * @描述:
    *   获取用户及其关注用户的最新微博
    * 
    * @参数:
    *   access_token: 通过OAuth2认证得到的身份标识
    *   since_id: 若指定此参数，则返回ID比since_id大的微博（即比since_id时间晚的微博），默认为0
    *   max_id: 若指定此参数，则返回ID小于或等于max_id的微博，默认为0
    *   count: 单页返回的记录条数，最大不超过100，超过100以100处理，默认为20
    *   page: 返回结果的页码，默认为1
    *
    * @返回:
    *   请求成功返回用户及其关注用户的最新发布的微博, JSON格式数据，具体内容参考：http://open.weibo.com/wiki/2/statuses/home_timeline
    *   请求失败返回空
    */
    string getConcernStatuses(1: string access_token, 2: i64 since_id, 3: i64 max_id, 4: i32 count, 5: i32 page), 

    /**
    * @描述:
    *   获取微博的评论数、转发数和点赞数（点赞数暂不支持获取）
    * 
    * @参数:
    *   access_token: 通过OAuth2认证得到的身份标识
    *   ids: 需要获取数据的微博ID，多个之间用逗号分隔，最多不超过100个
    *
    * @返回:
    *   请求成功返回对应微博ID的评论数、转发数和点赞数列表, JSON格式数据，具体内容参考：http://open.weibo.com/wiki/2/statuses/count
    *   请求失败返回空
    */
    string getStatusInteractCount(1: string access_token, 2: string ids), 

    /**
    * @描述:
    *   对一条微博进行评论
    * 
    * @参数:
    *   access_token: 通过OAuth2认证得到的身份标识
    *   comments: 评论内容，必须做URLencode，内容不超过140个汉字
    *   msg_id: 需要评论的微博ID
    *   is_comment_ori: 当评论转发微博时，是否评论给原微博，0：否、1：是，默认为0
    *
    * @返回:
    *   请求成功返回对应微博ID的评论数、转发数和点赞数列表, JSON格式数据，具体内容参考：http://open.weibo.com/wiki/2/statuses/count
    *   请求失败返回空
    */
    string createComments(1: string access_token, 2: string comments, 3: i64 msg_id, 4: i32 is_comment_ori),
    /**
    * @描述:
    *   获取微博表情
    * 
    * @参数:
    *   access_token: 通过OAuth2认证得到的身份标识
    *   emotion_type: 表情类型，现在提供face，普通表情、ani，魔法表情、cartoon，动漫表情
    *
    * @返回:
    *   请求成功返回对应表情类型的列表, JSON格式数据，具体内容参考：http://open.weibo.com/wiki/2/emotions
    *   请求失败返回空
    */
    string getEmotions(1: string access_token, 2: string emotion_type),
}

