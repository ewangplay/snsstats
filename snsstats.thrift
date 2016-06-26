/* 
 * thrift interface for snsstats
 */

namespace cpp jzlservice.snsstats
namespace go jzlservice.snsstats
namespace py jzlservice.snsstats
namespace php jzlservice.snsstats
namespace perl jzlservice.snsstats
namespace java jzlservice.snsstats

/**
* 社交媒体状态统计服务
*/
service SNSStats {
    /** 
    * @描述: 
    *   服务连通性测试接口
    *
    * @返回: 
    *   返回pong表示服务正常；返回空或其它表示服务异常
    */
    string ping(),		            
}

