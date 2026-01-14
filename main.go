package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"watcharis/go-redis-memoryusage/cache"

	"github.com/redis/go-redis/v9"
)

const (
	// REDIS_HOST     = "master.ktb-core-bank-dcb-sit-redis.5ecofd.apse1.cache.amazonaws.com"
	// REDIS_PORT     = "6379"
	// REDIS_DB_0     = 0
	// REDIS_DB_2     = 2
	// REDIS_PASSWORD = "86558bd9-0dad-42b3-bbb5-acbfe1ba5990"
	// REDIS_TLS      = true

	REDIS_HOST     = "localhost"
	REDIS_PORT     = "6379"
	REDIS_DB_0     = 0
	REDIS_DB_2     = 2
	REDIS_PASSWORD = ""
	REDIS_TLS      = false
)

func main() {

	ctx := context.Background()

	redisAddr := fmt.Sprintf("%s:%s", REDIS_HOST, REDIS_PORT)
	redisClient, err := cache.ConnectRedisClient(ctx, redisAddr, REDIS_PASSWORD, REDIS_DB_0, REDIS_TLS)
	if err != nil {
		log.Panicf("cannot connect redis - [error]: %+v\n", err)
	}

	redisClient2, err := cache.ConnectRedisClient(ctx, redisAddr, REDIS_PASSWORD, REDIS_DB_2, REDIS_TLS)
	if err != nil {
		log.Panicf("cannot connect redis - [error]: %+v\n", err)
	}

	// redis DB 1
	redisKeyPattern := "EXAMPLE:PATTERN:*"
	patterns_db_0 := []string{
		redisKeyPattern,
	}
	ScanRedisFindMemoryUsage(ctx, redisClient, patterns_db_0)
	// ScanRedisGetTTL(ctx, redisClient, patterns_db_0)
	log.Printf("scan redis db 0 success\n\n")

	// redis DB 2
	patterns_db_2 := []string{
		"EVENT:EXAMPLE:*",
	}
	ScanRedisFindMemoryUsage(ctx, redisClient2, patterns_db_2)
	log.Printf("scan redis db 2 success\n\n")

}

func ScanRedisFindMemoryUsage(ctx context.Context, redisClient *redis.Client, patterns []string) {
	redisRepository := cache.NewRedisRepository(redisClient)
	for _, pattern := range patterns {
		var cursor uint64
		var maxSizeKB float64
		var minSizeKB float64
		var redisKeyMaxsize string
		var redisKeyMinsize string

		for {
			keys, newCursor, err := redisRepository.ScanByPattern(ctx, cursor, pattern, 1000)
			if err != nil {
				log.Printf("cannot scan redis by pattern - [error] %+v\n", err)
				return
			}

			for _, key := range keys {
				if key == "EXAMPLE:PATTERN:1234" {
					continue
				}

				sizeBytes, err := redisRepository.MemoryUsage(ctx, key)
				if err != nil {
					log.Printf("MEMORY USAGE error for key %s: %v", key, err)
					continue
				}

				sizeKB := float64(sizeBytes) / 1024.0
				// fmt.Printf("%s -> %.2f KB\n", key, sizeKB)
				// total += size

				GetMaxSize(ctx, key, sizeKB, &maxSizeKB, &redisKeyMaxsize)
				GetMinSize(ctx, key, sizeKB, &minSizeKB, &redisKeyMinsize)
			}

			cursor = newCursor
			if cursor == 0 {
				break
			}
		}

		fmt.Printf("pattern: %s\nrediskey_max_size: %s -> maxsize: %.2f KB\nrediskey_min_size: %s -> minsize: %.2f KB\n--------------------\n", pattern,
			redisKeyMaxsize,
			maxSizeKB,
			redisKeyMinsize,
			minSizeKB)
	}
}

func GetMaxSize(ctx context.Context, redisKey string, sizeKB float64, maxSizeKB *float64, redisKeyMaxsize *string) {
	if *maxSizeKB < sizeKB {
		*maxSizeKB = sizeKB
		*redisKeyMaxsize = redisKey
	}
}

func GetMinSize(ctx context.Context, redisKey string, sizeKB float64, minSizeKB *float64, redisKeyMinsize *string) {
	if *minSizeKB == 0 {
		*minSizeKB = sizeKB
		*redisKeyMinsize = redisKey
	} else if *minSizeKB < sizeKB {
		return
	} else if *minSizeKB > sizeKB {
		*minSizeKB = sizeKB
		*redisKeyMinsize = redisKey
	}
}

func ScanRedisGetTTL(ctx context.Context, redisClient *redis.Client, patterns []string) {
	redisRepository := cache.NewRedisRepository(redisClient)

	file, err := CreateFile()
	if err != nil {
		log.Panicf("ScanRedisGetTTL - cannot create file [error] : %+v\n", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, pattern := range patterns {
		var cursor uint64
		for {
			keys, newCursor, err := redisRepository.ScanByPattern(ctx, cursor, pattern, 1000)
			if err != nil {
				log.Printf("cannot scan redis by pattern - [error] %+v\n", err)
				return
			}

			for _, key := range keys {
				ttl, err := redisRepository.GetTTL(ctx, key)
				if err != nil {
					log.Printf("GET TTL error for key %s: %v", key, err)
					continue
				}

				if ttl > 0 {
					continue
				}

				content := fmt.Sprintf("%s\n", key)
				if _, err := writer.WriteString(content); err != nil {
					panic(err)
				}
			}

			cursor = newCursor
			if cursor == 0 {
				break
			}
		}

		if err := writer.Flush(); err != nil {
			fmt.Println("flush file error : ", err)
		}
	}
}

func CreateFile() (*os.File, error) {
	file, err := os.Create("rediskey-without-ttl.txt")
	if err != nil {
		return nil, err
	}
	return file, nil
}
