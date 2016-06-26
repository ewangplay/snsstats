package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

type SNSCache struct {
	pool *redis.Pool
}

func NewSNSCache() (*SNSCache, error) {
	cache := &SNSCache{}
	cache.pool = &redis.Pool{
		MaxIdle:     30,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			host, has_host := g_config.Get("redis.host")
			port, has_port := g_config.Get("redis.port")
			if !has_host || host == "" || !has_port || port == "" {
				return nil, fmt.Errorf("no redis config")
			}

			connStr := fmt.Sprintf("%v:%v", host, port)
			c, err := redis.Dial("tcp", connStr)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	return cache, nil
}

func (this *SNSCache) Release() {
	this.pool.Close()
}

func (this *SNSCache) Set(key, value string) error {
	conn := this.pool.Get()
	_, err := conn.Do("SET", key, value)
	if err != nil {
		return err
	}
	return nil
}

func (this *SNSCache) Get(key string) (string, error) {
	conn := this.pool.Get()
	value, err := redis.String(conn.Do("GET", key))
	if err != nil {
		return "", err
	}
	return value, nil
}
