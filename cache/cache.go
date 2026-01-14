package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisRepository interface {
	MemoryUsage(ctx context.Context, key string) (int64, error)
	ScanByPattern(ctx context.Context, cursor uint64, pattern string, count int64) ([]string, uint64, error)
	GetTTL(ctx context.Context, key string) (time.Duration, error)
	Expire(ctx context.Context, key string, expiration time.Duration) (bool, error)
}

type redisRepository struct {
	redisClient *redis.Client
}

func NewRedisRepository(redisClient *redis.Client) RedisRepository {
	return &redisRepository{
		redisClient: redisClient,
	}
}

func (r *redisRepository) MemoryUsage(ctx context.Context, key string) (int64, error) {
	// result int64 instead memory usage bytes
	result, err := r.redisClient.MemoryUsage(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return result, err
}

func (r *redisRepository) ScanByPattern(ctx context.Context, cursor uint64, pattern string, count int64) ([]string, uint64, error) {
	result, newcursor, err := r.redisClient.Scan(ctx, cursor, pattern, count).Result()
	if err != nil {
		return nil, 0, err
	}
	return result, newcursor, nil
}

func (r *redisRepository) GetTTL(ctx context.Context, key string) (time.Duration, error) {
	result, err := r.redisClient.TTL(ctx, key).Result()
	if err != nil {
		return 0, err
	}

	return result, nil
}

func (r *redisRepository) Expire(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	result, err := r.redisClient.Expire(ctx, key, expiration).Result()
	if err != nil {
		return false, err
	}
	return result, nil
}
