package server

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func NewRedisLockManager(client *redis.Client, namespace string, ttl time.Duration, getLatestVersionFromLog LatestVersionLoader) LockManager {
	return func(ctx context.Context, streamId string, expectedVersion int64) error {

		// Compute a namespaced key to avoid application conflicts
		key := namespace + "||" + streamId

		// Start watching the key. If it changes while we are executing the
		// change, then an optimistic locking condition will occur. This
		// means that we have lost our slot and we have to abort the write.
		return client.Watch(ctx, func(tx *redis.Tx) error {

			// Check if the key exists and exit if an unexpected error occurs
			value, err := tx.Get(ctx, key).Result()
			if err != nil && err != redis.Nil {
				return err
			}

			var actual int64
			// Key does not exist in the cache
			if err == redis.Nil {
				// Load the latest version from the persistence, if it exists
				actual, err = getLatestVersionFromLog(ctx, streamId)
				if err != nil {
					return err
				}
			} else {
				// The stream exists in the cache, parse the response
				actual, err = strconv.ParseInt(value, 10, 64)
				if err != nil {
					return err
				}
			}

			// The record exists but it is not the expected version
			// Return an optimistic locking error
			if actual != expectedVersion {
				return OptimisticLockingError()
			}

			// We are all fine, update the cache with the new version
			// Using a pipeline to minimize the round-trips to Redis
			_, err = tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				err := pipe.Set(ctx, key, actual+1, ttl).Err()
				return err
			})
			return err

		}, key)
	}
}
