package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/pborman/uuid"
	"github.com/wyattjoh/jobs"
)

var pool *redis.Pool

func TestMain(m *testing.M) {
	addr, err := url.Parse(os.Getenv("REDIS_DSN"))
	if err != nil {
		log.Fatalf("Expected no error, got %s", err.Error())
	}

	redisPool := &redis.Pool{
		IdleTimeout: 30 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr.Host)
			if err != nil {
				return nil, err
			}
			if addr.User != nil {
				password, _ := addr.User.Password()
				if password != "" {
					if _, err = c.Do("AUTH", password); err != nil {
						c.Close()
						return nil, err
					}
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < 1*time.Minute {
				return nil
			}

			// Test that we can ping if the last time we tested this connection was
			// more than 1 minute ago.
			if _, err := c.Do("PING"); err != nil {
				return err
			}

			return nil
		},
	}
	defer redisPool.Close()

	pool = redisPool

	os.Exit(m.Run())
}

func TestFailedJobs(t *testing.T) {
	testList := uuid.New()

	conn := pool.Get()
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const payload = 5

	_, err := jobs.Push(ctx, conn, testList, payload)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}
	t.Logf("Expected no error, got nil")

	attempts := 0
	failedErr := errors.New("my failure error")

	f := func(ctx context.Context, msg jobs.Message) error {
		attempts++

		if attempts > jobs.MaxAttempts {
			t.Fatalf("Expected job not to be executed more than MaxAttempts[%d], it was Attempts[%d]", attempts, jobs.MaxAttempts)
		}

		if attempts-1 != msg.Attempts {
			t.Fatalf("Expected job to have an attempt counter equal to %d, it was %d", attempts-1, msg.Attempts)
		}
		t.Logf("Expected job to have an attempt counter equal to %d, it was", attempts-1)

		return failedErr
	}

	for i := 0; i <= jobs.MaxAttempts+1; i++ {
		err := jobs.Pull(ctx, conn, testList, f)
		if i > jobs.MaxAttempts && err != jobs.ErrEmpty {
			t.Fatalf("Expected there to be an error of %s when we exeeded the max attempts, there was %s", jobs.ErrEmpty, err)
		} else if err != jobs.ErrEmpty && err != failedErr {
			t.Fatalf("Expeced a specific error %s, got %s", failedErr, err)
		}

		if i == jobs.MaxAttempts && err == jobs.ErrEmpty {
			t.Logf("Expected there to be an error of %s when we exeeded the max attempts, there was", jobs.ErrEmpty)
		}

		if err == failedErr {
			t.Logf("Expeced a specific error %s, got it", failedErr)
		}
	}

	if attempts != 5 {
		t.Fatalf("Expected the job to be tried %d times, it was tried %d times", jobs.MaxAttempts, attempts)
	}
	t.Logf("Expected the job to be tried %d times, it was", jobs.MaxAttempts)
}

func TestJobQueue(t *testing.T) {
	testList := uuid.New()

	conn := pool.Get()
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pl := make([]string, 5)
	for i := range pl {
		pl[i] = uuid.New()
	}

	for i, p := range pl {

		if _, err := jobs.Push(ctx, conn, testList, p); err != nil {
			t.Fatalf("Expected no error, got %s", err.Error())
		}
		t.Logf("Expected no error, got nil")

		listCount, err := redis.Int(conn.Do("LLEN", fmt.Sprintf(jobs.ActiveListFormat, testList)))
		if err != nil {
			t.Fatalf("Expected no error, got %s", err.Error())
		}
		t.Logf("Expected no error, got nil")

		pendingListCount, err := redis.Int(conn.Do("LLEN", fmt.Sprintf(jobs.ProcessingListFormat, testList)))
		if err != nil {
			t.Fatalf("Expected no error, got %s", err.Error())
		}
		t.Logf("Expected no error, got nil")

		if listCount != i+1 {
			t.Fatalf("Expected list to be length %d, got %d", i+1, listCount)
		}
		t.Logf("Expected list to be length %d, got %d", i+1, listCount)

		if pendingListCount != 0 {
			t.Fatalf("Expected list to be length 0, got %d", pendingListCount)
		}
		t.Logf("Expected list to be length 0, got %d", pendingListCount)

	}

	for i := len(pl) - 1; i >= 0; i-- {
		err := jobs.Pull(ctx, conn, testList, func(ctx context.Context, msg jobs.Message) error {
			var p string
			if err := msg.Unmarshal(&p); err != nil {
				t.Fatalf("Expected no error, got %s", err.Error())
			}
			t.Logf("Expected no error, got nil")

			if p != pl[i] {
				t.Fatalf("Expected payload to be %s, it was %s", pl[i], p)
			}
			t.Logf("Expected payload to be %s, it was %s", pl[i], p)

			stats, err := jobs.Stats(ctx, conn, testList)
			if err != nil {
				t.Fatalf("Expected no error, got %s", err.Error())
			}
			t.Logf("Expected no error, got nil")

			if stats.Active != i {
				t.Fatalf("Expected unprocessed jobs list to be length %d, got %d", i, stats.Active)
			}
			t.Logf("Expected unprocessed jobs list to be length %d, got %d", i, stats.Active)

			if stats.Processing != 1 {
				t.Fatalf("Expected pending jobs list to be length 1, got %d", stats.Processing)
			}
			t.Logf("Expected pending jobs list to be length 1, got %d", stats.Processing)

			if stats.Failed != 0 {
				t.Fatalf("Expected failed jobs list to be length 0, got %d", stats.Failed)
			}
			t.Logf("Expected failed jobs list to be length 0, got %d", stats.Failed)

			return nil
		})
		if err != nil {
			t.Fatalf("Expected no error, got %s", err.Error())
		}
		t.Logf("Expected no error, got nil")

		stats, err := jobs.Stats(ctx, conn, testList)
		if err != nil {
			t.Fatalf("Expected no error, got %s", err.Error())
		}
		t.Logf("Expected no error, got nil")

		if stats.Active != i {
			t.Fatalf("Expected unprocessed jobs list to be length %d, got %d", i, stats.Active)
		}
		t.Logf("Expected unprocessed jobs list to be length %d, got %d", i, stats.Active)

		if stats.Processing != 0 {
			t.Fatalf("Expected pending jobs list to be length 0, got %d", stats.Processing)
		}
		t.Logf("Expected pending jobs list to be length 0, got %d", stats.Processing)

		if stats.Failed != 0 {
			t.Fatalf("Expected failed jobs list to be length 0, got %d", stats.Failed)
		}
		t.Logf("Expected failed jobs list to be length 0, got %d", stats.Failed)

	}

	err := jobs.Pull(ctx, conn, testList, nil)
	if err != jobs.ErrEmpty {
		t.Fatalf("Expected error %s to be returned, got %s", jobs.ErrEmpty, err)
	}
	t.Logf("Expected error %s to be returned, got %s", jobs.ErrEmpty, err)
}
