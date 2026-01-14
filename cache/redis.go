package cache

import (
	"context"
	"crypto/tls"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

func ConnectRedisClient(ctx context.Context, addr, password string, db int, flagTls bool) (*redis.Client, error) {

	options := &redis.Options{
		Addr:        addr,
		Password:    password,
		DB:          db,
		PoolSize:    30,
		MaxRetries:  30,
		ReadTimeout: time.Duration(30) * time.Second,
	}

	if !flagTls {
		options.TLSConfig = nil
	} else {
		options.TLSConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true,
		}
	}

	rdb := redis.NewClient(options)

	ping, err := rdb.Ping(ctx).Result()
	if err != nil {
		return nil, errors.Wrap(err, "Cannot connect to redis")
	}

	log.Printf("redis ping: %+v\n", ping)
	return rdb, nil
}
