package server_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/spoletum/annales/pkg/server"
	"github.com/stretchr/testify/assert"
)

func TestNewRedisLockManager(t *testing.T) {
	ctx := context.Background()
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	namespace := "testNamespace"
	ttl := 5 * time.Second
	streamId := "testStream"

	t.Run("Success case: Key does not exist and is successfully created", func(t *testing.T) {
		loadFunc := func(ctx context.Context, streamId string) (int64, error) {
			return int64(0), nil
		}
		lockManager := server.NewRedisLockManager(redisClient, namespace, ttl, loadFunc)
		err := lockManager(ctx, streamId, 0)
		assert.NoError(t, err)

		// Check that the value in Redis is correct
		key := namespace + "||" + streamId
		value, err := redisClient.Get(ctx, key).Int64()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), value)
	})

	t.Run("Success case: Key exists and is successfully updated", func(t *testing.T) {
		loadFunc := func(ctx context.Context, streamId string) (int64, error) {
			return int64(1), nil
		}
		lockManager := server.NewRedisLockManager(redisClient, namespace, ttl, loadFunc)
		err := lockManager(ctx, streamId, 1)
		assert.NoError(t, err)

		// Check that the value in Redis is correct
		key := namespace + "||" + streamId
		value, err := redisClient.Get(ctx, key).Int64()
		assert.NoError(t, err)
		assert.Equal(t, int64(2), value)
	})

	t.Run("Failure case: Key does not exist, but an unexpected error occurs when loading from the persistence", func(t *testing.T) {
		redisClient.Del(ctx, namespace+"||"+streamId)
		loadFunc := func(ctx context.Context, streamId string) (int64, error) {
			return int64(0), errors.New("Unexpected error")
		}
		lockManager := server.NewRedisLockManager(redisClient, namespace, ttl, loadFunc)
		err := lockManager(ctx, streamId, 0)
		assert.Error(t, err)
	})

	t.Run("Failure case: Key exists, but the expected version does not match the actual version", func(t *testing.T) {
		loadFunc := func(ctx context.Context, streamId string) (int64, error) {
			return int64(1), nil
		}
		lockManager := server.NewRedisLockManager(redisClient, namespace, ttl, loadFunc)
		err := lockManager(ctx, streamId, 10)
		assert.Error(t, err)
		assert.Equal(t, server.OptimisticLockingError(), err)
	})

	t.Run("Failure case: Key exists with non-numeric value", func(t *testing.T) {
		// Set a non-numeric value for the key in Redis
		key := namespace + "||" + streamId
		err := redisClient.Set(ctx, key, "non-numeric", ttl).Err()
		assert.NoError(t, err)

		loadFunc := func(ctx context.Context, streamId string) (int64, error) {
			return int64(0), nil
		}
		lockManager := server.NewRedisLockManager(redisClient, namespace, ttl, loadFunc)
		err = lockManager(ctx, streamId, 0)
		assert.Error(t, err)

		// Check that the value in Redis is still the non-numeric value
		value, err := redisClient.Get(ctx, key).Result()
		assert.NoError(t, err)
		assert.Equal(t, "non-numeric", value)
	})
}

func BenchmarkRedisLockManagerAlwaysNewStream(b *testing.B) {
	ctx := context.Background()
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	namespace := "benchmarkNamespace"
	ttl := 5 * time.Minute

	loadFunc := func(ctx context.Context, streamId string) (int64, error) {
		return int64(0), nil
	}
	lockManager := server.NewRedisLockManager(redisClient, namespace, ttl, loadFunc)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		streamId := uuid.NewString()
		err := lockManager(ctx, streamId, 0)
		if err != nil {
			b.Errorf("Error during benchmark: %v", err)
		}
	}
}

// This benchmark keeps on locking a stream that exists in cache
func BenchmarkRedisLockManagerExistingStream(b *testing.B) {
	ctx := context.Background()
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	namespace := "benchmarkNamespace"
	ttl := 5 * time.Minute

	loadFunc := func(ctx context.Context, streamId string) (int64, error) {
		return int64(0), nil
	}
	lockManager := server.NewRedisLockManager(redisClient, namespace, ttl, loadFunc)
	streamId := uuid.NewString()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := lockManager(ctx, streamId, int64(n))
		if err != nil {
			b.Errorf("Error during benchmark: %v", err)
		}
	}
}
