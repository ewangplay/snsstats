package main

import (
	"flag"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/ewangplay/jzlconfig"
	"github.com/outmana/log4jzl"
	"jzlservice/snsstats"
	"os"
	"strconv"
)

//global object
var g_config jzlconfig.JZLConfig
var g_logger *log4jzl.Log4jzl
var g_snsCache *SNSCache
var g_nsqConsumerMgr *NSQConsumerMgr
var g_queueCapacity uint64
var g_mysqladaptor *MysqlDBAdaptor
var g_weiboSenderClient *WeiboSenderClient
var g_weixinSenderClient *WeixinSenderClient

func Usage() {
	fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [--config path_to_config_file]")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr)
	os.Exit(0)
}

func main() {
	var err error
	var ok bool

	//parse command line
	var configFile string
	flag.Usage = Usage
	flag.StringVar(&configFile, "config", "snsstats.conf", "specified config filename")
	flag.Parse()

	fmt.Println("config file: ", configFile)

	//read config file
	if err = g_config.Read(configFile); err == nil {
		fmt.Println(g_config)
	} else {
		fmt.Println("Read config file fail.", err)
		os.Exit(1)
	}

	//init logger
	g_logger, err = log4jzl.New("snsstats")
	if err != nil {
		fmt.Println("Open log file fail.", err)
		os.Exit(1)
	}

	//init log level object
	g_logLevel, err = NewLogLevel()
	if err != nil {
		LOG_ERROR("Craete Log level error: %v", err)
		os.Exit(1)
	}

	//init the queue capacity
	cap, ok := g_config.Get("service.queue.capacity")
	if !ok || cap == "" {
		cap = "1000"
	}
	g_queueCapacity, err = strconv.ParseUint(cap, 0, 0)
	if err != nil {
		g_queueCapacity = 1000
	}

	LOG_INFO("发送队列的最大容量设置为%v", g_queueCapacity)

	//init cache manager
	g_snsCache, err = NewSNSCache()
	if err != nil {
		LOG_ERROR("创建SNSCache对象失败，失败原因: %v", err)
		os.Exit(1)
	}

	//init nsq consumer manager
	g_nsqConsumerMgr, err = NewNSQConsumerMgr()
	if err != nil {
		fmt.Println("create NSQConsumerMgr object fail.", err)
		os.Exit(1)
	}
	g_nsqConsumerMgr.Init()
	g_nsqConsumerMgr.Run()
	defer g_nsqConsumerMgr.Release()

	//init mysql db adaptor
	g_mysqladaptor, err = NewMysqlDBAdaptor()
	if err != nil {
		fmt.Println("create MysqlDBAdaptor object fail.", err)
		os.Exit(1)
	}
	defer g_mysqladaptor.Release()

	//init weibo sender client
	g_weiboSenderClient, _ = NewWeiboSenderClient()
	g_weixinSenderClient, _ = NewWeixinSenderClient()

	//format the server listening newwork address
	var networkAddr string
	serviceIp, serviceIPIsSet := g_config.Get("service.addr")
	servicePort, servicePortIsSet := g_config.Get("service.port")
	if serviceIPIsSet && servicePortIsSet {
		networkAddr = fmt.Sprintf("%s:%s", serviceIp, servicePort)
	} else {
		networkAddr = "127.0.0.1:19090"
	}

	//startup snsstats service
	transportFactory := thrift.NewTBufferedTransportFactory(1024)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	serverTransport, err := thrift.NewTServerSocket(networkAddr)
	if err != nil {
		fmt.Println("create socket listening fail.", err)
		os.Exit(1)
	}
	handler := &SNSStatsImpl{}
	processor := snsstats.NewSNSStatsProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)

	fmt.Println("snsstats server working on", networkAddr)
	LOG_INFO("snsstats服务启动，监听地址：%v", networkAddr)

	server.Serve()
}
