// Package jobs is a simple implementation of a job queue using redis's powerful
// RPOPLPUSH command to ensure reliable completion of jobs.
//
// Currently not implemented but planned is functions to access the failed list
// and a way to mark a job as failed and to stop retrying.
package jobs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
)

var (

	// ErrEmpty is returned when there is no job to pull.
	ErrEmpty = errors.New("there was no job to pull")
)

// Message is used to encapsulate the job's data.s
type Message struct {
	ID       string
	Data     []byte
	Name     string `json:"-"`
	Attempts int    `json:"-"`
}

// Unmarshal extracts the payload from the Message.
func (m Message) Unmarshal(dst interface{}) error {
	return json.Unmarshal(m.Data, dst)
}

var (

	// MaxAttempts declares how many times that a job can be retried before it is
	// considered failed.
	MaxAttempts = 5
)

const (

	// ActiveListFormat is the format used to indicate active jobs.
	ActiveListFormat = "jobs:%s:active"

	// ProcessingListFormat is the format for the list to indicate processing
	// jobs.
	ProcessingListFormat = "jobs:%s:processing"

	// FailedListFormat is a list containing failed jobs that have exceeded the
	// failure attempts.
	FailedListFormat = "jobs:%s:failed"

	// JobFormat is the details for a specific job stored in a hash set. This is
	// cleared when the job has been sucesfully processed.
	JobFormat = "jobs:%s:job:%s"
)

// Push pushes a job into a list to be pulled at a later time. It will return
// the job id.

// Push will marshal the incoming payload and create a Message for it. It will
// then push that into the active list to be pulled on subsequent Pull's. It
// will return the Message ID for tracing.
func Push(ctx context.Context, con redis.Conn, name string, payload interface{}) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	// Create the message object to push.
	msg := Message{
		ID:   uuid.New(),
		Data: data,
	}

	if err := queue(ctx, con, name, msg); err != nil {
		return "", err
	}

	return msg.ID, nil
}

// queue will push the job into the queue.
func queue(ctx context.Context, con redis.Conn, name string, msg Message) error {

	// Marshal the message to be passed down.
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if _, err := con.Do("RPUSH", fmt.Sprintf(ActiveListFormat, name), b); err != nil {
		return err
	}

	return nil
}

// attempt marks that a given job has attempted to process a given job.
func attempt(ctx context.Context, con redis.Conn, name string, msg Message) error {

	// Mark that another attempt has been made.
	if _, err := con.Do("HINCRBY", fmt.Sprintf(JobFormat, name, msg.ID), "attempts", 1); err != nil {
		return err
	}

	return nil
}

// requeue puts the job back into the active jobs list after other jobs.
func requeue(ctx context.Context, con redis.Conn, name string, msg Message) error {

	// Mark that another attempt has been made.
	if err := attempt(ctx, con, name, msg); err != nil {
		return err
	}

	// Add the job back to the queue list.
	if err := queue(ctx, con, name, msg); err != nil {
		return err
	}

	return nil
}

// failed marks that the job has failed either enough times or in a way that
// indicates that it should not be retried.
func failed(ctx context.Context, con redis.Conn, name string, msg Message) error {

	// Mark that another attempt has been made.
	if err := attempt(ctx, con, name, msg); err != nil {
		return err
	}

	// Marshal the message to be passed down.
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if _, err := con.Do("RPUSH", fmt.Sprintf(FailedListFormat, name), b); err != nil {
		return err
	}

	return nil
}

// finish marks a given job as processed and finished.
func finish(ctx context.Context, con redis.Conn, name string, msg Message) error {

	// Remove the attempt history.
	if _, err := con.Do("DEL", fmt.Sprintf(JobFormat, name, msg.ID)); err != nil {
		return err
	}

	return nil
}

// attempts looks up the number of attempts on the specific job. If the count
// isn't found, it returns 0 and no error.
func attempts(ctx context.Context, con redis.Conn, name string, msg Message) (int, error) {
	attempts, err := redis.Int(con.Do("HGET", fmt.Sprintf(JobFormat, name, msg.ID), "attempts"))
	if err != nil {
		if err == redis.ErrNil {
			return 0, nil
		}

		return 0, err
	}

	return attempts, nil
}

// Pull will try to get the next job to process, an will return with an ErrEmpty
// if there was no job to pull. The work function provided will get a copy of
// the message being processed, and upon sucesfull return from that function
// it will mark the message as processed. If the work function return an error,
// that message will be re-added to the active queue for a maximum of
// MaxAttempts times at which point it will be placed into the Failed list.
func Pull(ctx context.Context, con redis.Conn, name string, work func(context.Context, Message) error) error {
	b, err := redis.Bytes(con.Do("RPOPLPUSH", fmt.Sprintf(ActiveListFormat, name), fmt.Sprintf(ProcessingListFormat, name)))
	if err != nil {
		if err == redis.ErrNil {
			return ErrEmpty
		}

		return errors.Wrap(err, "cannot pull the job from redis")
	}

	// Unmarshal the original message.
	var msg Message
	if err := json.Unmarshal(b, &msg); err != nil {
		return errors.Wrap(err, "cannot unmarshal the message")
	}

	// Look up the number of attempts on this entry.
	attempts, err := attempts(ctx, con, name, msg)
	if err != nil {
		return errors.Wrap(err, "cannot get the number of attempts on the job")
	}

	msg.Name = name
	msg.Attempts = attempts

	// Perform the action on the message.
	err = work(ctx, msg)

	// Handle the error returned.
	if err != nil {

		// An error occured when processing the job, we should therefore requeue the
		// job unless we have attempted this already too many times, don't requeue
		// it, instead mark this as a failed job.
		if attempts+1 >= MaxAttempts {
			if err := failed(ctx, con, name, msg); err != nil {

				// We couldn't mark the job as failed, should error out now.
				return errors.Wrap(err, "cannot mark the job as failed")
			}
		} else if err := requeue(ctx, con, name, msg); err != nil {

			// We couldn't requeue the job, should error out now.
			return errors.Wrap(err, "cannot requeue the job")
		}
	}

	// The message was finished processing, failed or not, we should remove it
	// from the processing list.
	if _, err := con.Do("LREM", fmt.Sprintf(ProcessingListFormat, name), 0, b); err != nil {
		return err
	}

	return err
}

// JobStats contains the statistics for the current job name.
type JobStats struct {
	Processing int
	Failed     int
	Active     int
}

// Stats returns the JobStats for a given job name.
func Stats(ctx context.Context, con redis.Conn, name string) (*JobStats, error) {
	con.Send("LLEN", fmt.Sprintf(ProcessingListFormat, name))
	con.Send("LLEN", fmt.Sprintf(FailedListFormat, name))
	con.Send("LLEN", fmt.Sprintf(ActiveListFormat, name))

	if err := con.Flush(); err != nil {
		return nil, err
	}

	processing, err := redis.Int(con.Receive())
	if err != nil {
		return nil, err
	}

	failed, err := redis.Int(con.Receive())
	if err != nil {
		return nil, err
	}

	active, err := redis.Int(con.Receive())
	if err != nil {
		return nil, err
	}

	return &JobStats{
		Processing: processing,
		Failed:     failed,
		Active:     active,
	}, nil
}
