// pkg/redismq/consumer.go

package redismq

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MessageHandler 消息处理函数
type MessageHandler func(msg *Message) error

// Consumer 表示一个消费者
type Consumer struct {
	id        string
	cancel    context.CancelFunc
	done      chan struct{}
	handler   MessageHandler
	isRunning bool
}

// ConsumerGroup 管理一组消费者
type ConsumerGroup struct {
	mq        *RedisMQ
	consumers map[string]*Consumer
	mu        sync.Mutex
}

// newConsumerGroup 创建一个新的消费者组
func newConsumerGroup(mq *RedisMQ) *ConsumerGroup {
	return &ConsumerGroup{
		mq:        mq,
		consumers: make(map[string]*Consumer),
	}
}

// AddConsumer 添加一个新的消费者
func (cg *ConsumerGroup) AddConsumer(handler MessageHandler) (string, error) {
	if handler == nil {
		return "", ErrHandlerNil
	}

	cg.mu.Lock()
	defer cg.mu.Unlock()

	// 生成唯一的消费者ID
	consumerID := fmt.Sprintf("consumer-%d", time.Now().UnixNano())

	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())

	// 创建完成通知通道
	done := make(chan struct{})

	// 创建消费者
	consumer := &Consumer{
		id:        consumerID,
		cancel:    cancel,
		done:      done,
		handler:   handler,
		isRunning: true,
	}

	// 存储消费者
	cg.consumers[consumerID] = consumer

	// 启动消费者协程
	go func() {
		defer close(done)

		// 将传入的处理函数封装为内部处理函数
		internalHandler := func(msgStr string) error {
			var msg Message
			if err := msg.UnmarshalBinary([]byte(msgStr)); err != nil {
				return WrapError(err, "解析消息失败")
			}
			return handler(&msg)
		}

		err := cg.mq.consume(ctx, internalHandler)
		if err != nil && err != context.Canceled {
			if cg.mq.config.EnableLogging {
				cg.mq.config.Logger.Error("消费者", consumerID, "停止，错误:", err)
			}
		}
	}()

	if cg.mq.config.EnableLogging {
		cg.mq.config.Logger.Info("消费者", consumerID, "已启动")
	}

	return consumerID, nil
}

// StopConsumer 停止指定ID的消费者
func (cg *ConsumerGroup) StopConsumer(consumerID string) error {
	cg.mu.Lock()
	consumer, exists := cg.consumers[consumerID]
	cg.mu.Unlock()

	if !exists {
		return WrapError(ErrConsumerNotFound, "消费者ID: %s", consumerID)
	}

	if !consumer.isRunning {
		return WrapError(ErrConsumerNotRunning, "消费者ID: %s", consumerID)
	}

	// 取消消费者的上下文
	consumer.cancel()

	// 等待消费者完成
	<-consumer.done

	// 更新消费者状态
	cg.mu.Lock()
	if consumer, exists = cg.consumers[consumerID]; exists {
		consumer.isRunning = false
	}
	cg.mu.Unlock()

	if cg.mq.config.EnableLogging {
		cg.mq.config.Logger.Info("消费者", consumerID, "已停止")
	}

	return nil
}

// StopAllConsumers 停止所有消费者
func (cg *ConsumerGroup) StopAllConsumers() error {
	cg.mu.Lock()
	consumerIDs := make([]string, 0, len(cg.consumers))
	for id, consumer := range cg.consumers {
		if consumer.isRunning {
			consumerIDs = append(consumerIDs, id)
		}
	}
	cg.mu.Unlock()

	var lastErr error
	for _, id := range consumerIDs {
		if err := cg.StopConsumer(id); err != nil {
			lastErr = err
			if cg.mq.config.EnableLogging {
				cg.mq.config.Logger.Error("停止消费者", id, "错误:", err)
			}
		}
	}

	return lastErr
}

// GetConsumerCount 获取当前活跃的消费者数量
func (cg *ConsumerGroup) GetConsumerCount() int {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	count := 0
	for _, consumer := range cg.consumers {
		if consumer.isRunning {
			count++
		}
	}

	return count
}

// GetConsumerIDs 获取当前活跃的消费者ID列表
func (cg *ConsumerGroup) GetConsumerIDs() []string {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	ids := make([]string, 0, len(cg.consumers))
	for id, consumer := range cg.consumers {
		if consumer.isRunning {
			ids = append(ids, id)
		}
	}

	return ids
}

// IsConsumerRunning 检查指定ID的消费者是否运行中
func (cg *ConsumerGroup) IsConsumerRunning(consumerID string) bool {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	consumer, exists := cg.consumers[consumerID]
	return exists && consumer.isRunning
}
