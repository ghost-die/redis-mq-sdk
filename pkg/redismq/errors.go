// pkg/redismq/errors.go

package redismq

import (
	"errors"
	"fmt"
)

var (
	// ErrEmptyQueueName 队列名称为空错误
	ErrEmptyQueueName = errors.New("队列名称不能为空")
	// ErrInvalidPriority 无效的优先级错误
	ErrInvalidPriority = errors.New("无效的优先级")
	// ErrConnectionFailed Redis连接失败错误
	ErrConnectionFailed = errors.New("Redis连接失败")
	// ErrConsumerNotFound 消费者不存在错误
	ErrConsumerNotFound = errors.New("消费者不存在")
	// ErrConsumerNotRunning 消费者未运行错误
	ErrConsumerNotRunning = errors.New("消费者未运行")
	// ErrQueueClosed 队列已关闭错误
	ErrQueueClosed = errors.New("队列已关闭")
	// ErrHandlerNil 处理器为空错误
	ErrHandlerNil = errors.New("消息处理器不能为空")
)

// WrapError 将错误包装成带前缀的错误
func WrapError(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	prefix := fmt.Sprintf(format, args...)
	return fmt.Errorf("%s: %w", prefix, err)
}
