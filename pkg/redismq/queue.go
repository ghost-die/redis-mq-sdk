// pkg/redismq/queue.go

package redismq

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisMQ 基于Redis的消息队列实现
type RedisMQ struct {
	client          *redis.Client
	config          MQConfig
	mainQueueBase   string
	processingQueue string
	deadQueue       string
	consumerGroup   *ConsumerGroup
	closed          bool
	ctx             context.Context
	cancelFunc      context.CancelFunc
	monitor         *Monitor // 新增监控器
}

// NewRedisMQ 创建一个新的消息队列实例
func NewRedisMQ(config MQConfig) (*RedisMQ, error) {
	if config.QueueName == "" {
		return nil, ErrEmptyQueueName
	}

	if config.MaxPriority <= 0 {
		return nil, ErrInvalidPriority
	}

	if int(config.DefaultPriority) >= config.MaxPriority {
		return nil, ErrInvalidPriority
	}

	// 创建Redis客户端
	client := redis.NewClient(&redis.Options{
		Addr:            config.Redis.Addr,
		Password:        config.Redis.Password,
		DB:              config.Redis.DB,
		MaxRetries:      config.Redis.MaxRetries,
		MinRetryBackoff: config.Redis.MinRetryBackoff,
		MaxRetryBackoff: config.Redis.MaxRetryBackoff,
		DialTimeout:     config.Redis.DialTimeout,
		ReadTimeout:     config.Redis.ReadTimeout,
		WriteTimeout:    config.Redis.WriteTimeout,
	})

	// 检查Redis连接
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, WrapError(ErrConnectionFailed, err.Error())
	}

	// 创建上下文
	bgCtx, bgCancel := context.WithCancel(context.Background())

	mq := &RedisMQ{
		client:          client,
		config:          config,
		mainQueueBase:   config.QueueName,
		processingQueue: config.QueueName + ":processing",
		deadQueue:       config.QueueName + ":dead",
		closed:          false,
		ctx:             bgCtx,
		cancelFunc:      bgCancel,
	}

	// 初始化消费者组
	mq.consumerGroup = newConsumerGroup(mq)

	// 初始化监控器
	mq.monitor = newMonitor(mq)

	if config.EnableLogging {
		config.Logger.Info("RedisMQ实例已创建，队列名称:", config.QueueName)
	}

	return mq, nil
}

// getQueueNameForPriority 根据优先级获取队列名称
func (mq *RedisMQ) getQueueNameForPriority(priority Priority) string {
	// 验证优先级
	if int(priority) >= mq.config.MaxPriority {
		priority = Priority(mq.config.MaxPriority - 1)
	}
	if priority < 0 {
		priority = 0
	}

	return fmt.Sprintf("%s:p%d", mq.mainQueueBase, priority)
}

// Publish 发布消息到队列，使用默认优先级
func (mq *RedisMQ) Publish(ctx context.Context, content string) error {
	return mq.PublishWithPriority(ctx, content, mq.config.DefaultPriority)
}

// PublishWithPriority 发布消息到队列，指定优先级
func (mq *RedisMQ) PublishWithPriority(ctx context.Context, content string, priority Priority) error {
	if mq.closed {
		return ErrQueueClosed
	}

	msg := NewMessage(content, priority)
	return mq.PublishMessage(ctx, msg)
}

// PublishMessage 发布消息对象到队列
func (mq *RedisMQ) PublishMessage(ctx context.Context, msg *Message) error {
	if mq.closed {
		return ErrQueueClosed
	}
	startTime := time.Now()

	// 获取队列名称
	queueName := mq.getQueueNameForPriority(msg.Priority)

	// 序列化消息
	msgBytes, err := msg.MarshalBinary()
	if err != nil {
		return WrapError(err, "消息序列化失败")
	}

	// 发布消息
	err = mq.client.RPush(ctx, queueName, msgBytes).Err()
	if err != nil {
		return WrapError(err, "发布消息失败")
	}

	if mq.config.EnableLogging {
		mq.config.Logger.Debug("消息已发布，ID:", msg.ID, "优先级:", msg.Priority)
	}
	// 记录发布性能
	if mq.monitor.IsRunning() {
		mq.monitor.RecordPublish(time.Since(startTime))
	}
	return nil
}

// PublishBatch 批量发布消息到队列，使用默认优先级
func (mq *RedisMQ) PublishBatch(ctx context.Context, contents []string) error {
	if mq.closed {
		return ErrQueueClosed
	}

	messages := make([]*Message, len(contents))
	for i, content := range contents {
		messages[i] = NewMessage(content, mq.config.DefaultPriority)
	}

	return mq.PublishBatchMessages(ctx, messages)
}

// PublishBatchWithPriority 批量发布消息到队列，指定优先级
func (mq *RedisMQ) PublishBatchWithPriority(ctx context.Context, contents []string, priority Priority) error {
	if mq.closed {
		return ErrQueueClosed
	}

	messages := make([]*Message, len(contents))
	for i, content := range contents {
		messages[i] = NewMessage(content, priority)
	}

	return mq.PublishBatchMessages(ctx, messages)
}

// PublishBatchMessages 批量发布消息对象到队列
func (mq *RedisMQ) PublishBatchMessages(ctx context.Context, messages []*Message) error {
	if mq.closed {
		return ErrQueueClosed
	}

	if len(messages) == 0 {
		return nil
	}

	// 按优先级分组
	priorityGroups := make(map[Priority][]string)
	for _, msg := range messages {
		// 序列化消息
		msgBytes, err := msg.MarshalBinary()
		if err != nil {
			return WrapError(err, "消息序列化失败")
		}

		priority := msg.Priority
		if int(priority) >= mq.config.MaxPriority {
			priority = Priority(mq.config.MaxPriority - 1)
		}
		if priority < 0 {
			priority = 0
		}

		priorityGroups[priority] = append(priorityGroups[priority], string(msgBytes))
	}

	// 使用管道批量操作
	pipe := mq.client.Pipeline()

	// 分别发布每个优先级组的消息
	for priority, msgStrs := range priorityGroups {
		queueName := mq.getQueueNameForPriority(priority)

		// 将[]string转换为[]interface{}
		args := make([]interface{}, len(msgStrs))
		for i, msg := range msgStrs {
			args[i] = msg
		}

		pipe.RPush(ctx, queueName, args...)
	}

	// 执行管道
	_, err := pipe.Exec(ctx)
	if err != nil {
		return WrapError(err, "批量发布消息失败")
	}

	if mq.config.EnableLogging {
		mq.config.Logger.Debug("已批量发布", len(messages), "条消息")
	}

	return nil
}

// tryGetMessageFromPriorityQueue 尝试从指定优先级的队列获取消息
func (mq *RedisMQ) tryGetMessageFromPriorityQueue(ctx context.Context, priority Priority) (string, error) {
	queueName := mq.getQueueNameForPriority(priority)

	// 使用BLMOVE原子性地移动消息
	message, err := mq.client.BLMove(
		ctx,
		queueName,
		mq.processingQueue,
		"LEFT", "RIGHT",
		time.Second, // 短超时，因为我们会轮询所有优先级
	).Result()

	return message, err
}

// consume 消费消息(阻塞式)，会从高优先级到低优先级顺序获取消息
func (mq *RedisMQ) consume(ctx context.Context, handler func(string) error) error {
	// 检查队列是否已关闭
	if mq.closed {
		return ErrQueueClosed
	}

	// 检查处理函数
	if handler == nil {
		return ErrHandlerNil
	}

	// 轮询获取消息
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-mq.ctx.Done():
			return ErrQueueClosed
		default:
			var message string
			var err error
			messageFetched := false

			// 从高优先级到低优先级尝试获取消息
			for priority := Priority(0); int(priority) < mq.config.MaxPriority; priority++ {
				message, err = mq.tryGetMessageFromPriorityQueue(ctx, priority)

				// 如果这个优先级队列中没有消息或超时，尝试下一个优先级
				if err == redis.Nil {
					continue
				}

				// 如果发生其他错误，记录并继续
				if err != nil {
					if mq.config.EnableLogging {
						mq.config.Logger.Error("从优先级队列", priority, "获取消息失败:", err)
					}
					continue
				}

				// 成功获取消息
				messageFetched = true
				break
			}

			// 如果所有优先级队列都没有消息，等待一段时间后继续
			if !messageFetched {
				time.Sleep(mq.config.ConsumerInterval)
				continue
			}
			startTime := time.Now()
			// 处理消息
			err = handler(message)
			if err != nil {
				if mq.config.EnableLogging {
					mq.config.Logger.Error("处理消息失败:", err)
				}

				// 将消息移动到死信队列
				moveErr := mq.client.RPush(ctx, mq.deadQueue, message).Err()
				if moveErr != nil && mq.config.EnableLogging {
					mq.config.Logger.Error("将消息移动到死信队列失败:", moveErr)
				}
			}
			// 记录消费性能
			if mq.monitor.IsRunning() {
				if err != nil {
					mq.monitor.RecordFailed()
				} else {
					mq.monitor.RecordConsume(time.Since(startTime))
				}
			}

			// 从处理队列中移除消息
			_, removeErr := mq.client.LRem(ctx, mq.processingQueue, 1, message).Result()
			if removeErr != nil && mq.config.EnableLogging {
				mq.config.Logger.Error("从处理队列中移除消息失败:", removeErr)
			}
		}
	}
}

// ConsumeAsync 创建并启动一个新的异步消费者
// 返回消费者ID，可用于后续停止该消费者
func (mq *RedisMQ) ConsumeAsync(handler MessageHandler) (string, error) {
	if mq.closed {
		return "", ErrQueueClosed
	}

	return mq.consumerGroup.AddConsumer(handler)
}

// StopConsumer 停止指定ID的消费者
func (mq *RedisMQ) StopConsumer(consumerID string) error {
	if mq.closed {
		return ErrQueueClosed
	}

	return mq.consumerGroup.StopConsumer(consumerID)
}

// StopAllConsumers 停止所有消费者
func (mq *RedisMQ) StopAllConsumers() error {
	return mq.consumerGroup.StopAllConsumers()
}

// GetConsumerCount 获取当前活跃的消费者数量
func (mq *RedisMQ) GetConsumerCount() int {
	return mq.consumerGroup.GetConsumerCount()
}

// GetConsumerIDs 获取当前活跃的消费者ID列表
func (mq *RedisMQ) GetConsumerIDs() []string {
	return mq.consumerGroup.GetConsumerIDs()
}

// Size 获取所有队列中的消息总数
func (mq *RedisMQ) Size(ctx context.Context) (int64, error) {
	if mq.closed {
		return 0, ErrQueueClosed
	}

	var total int64 = 0

	for priority := Priority(0); int(priority) < mq.config.MaxPriority; priority++ {
		queueName := mq.getQueueNameForPriority(priority)
		size, err := mq.client.LLen(ctx, queueName).Result()
		if err != nil {
			return 0, WrapError(err, "获取队列大小失败")
		}
		total += size
	}

	return total, nil
}

// SizeByPriority 获取指定优先级队列中的消息数量
func (mq *RedisMQ) SizeByPriority(ctx context.Context, priority Priority) (int64, error) {
	if mq.closed {
		return 0, ErrQueueClosed
	}

	if int(priority) >= mq.config.MaxPriority {
		return 0, WrapError(ErrInvalidPriority, "优先级 %d 超过最大优先级 %d", priority, mq.config.MaxPriority)
	}

	queueName := mq.getQueueNameForPriority(priority)
	return mq.client.LLen(ctx, queueName).Result()
}

// ProcessingSize 获取正在处理中的消息数量
func (mq *RedisMQ) ProcessingSize(ctx context.Context) (int64, error) {
	if mq.closed {
		return 0, ErrQueueClosed
	}

	return mq.client.LLen(ctx, mq.processingQueue).Result()
}

// DeadSize 获取死信队列中的消息数量
func (mq *RedisMQ) DeadSize(ctx context.Context) (int64, error) {
	if mq.closed {
		return 0, ErrQueueClosed
	}

	return mq.client.LLen(ctx, mq.deadQueue).Result()
}

// PeekProcessing 查看处理队列中的消息，不移除
func (mq *RedisMQ) PeekProcessing(ctx context.Context, start, stop int64) ([]*Message, error) {
	if mq.closed {
		return nil, ErrQueueClosed
	}

	msgStrings, err := mq.client.LRange(ctx, mq.processingQueue, start, stop).Result()
	if err != nil {
		return nil, WrapError(err, "获取处理中消息失败")
	}

	return mq.parseMessages(msgStrings)
}

// PeekDead 查看死信队列中的消息，不移除
func (mq *RedisMQ) PeekDead(ctx context.Context, start, stop int64) ([]*Message, error) {
	if mq.closed {
		return nil, ErrQueueClosed
	}

	msgStrings, err := mq.client.LRange(ctx, mq.deadQueue, start, stop).Result()
	if err != nil {
		return nil, WrapError(err, "获取死信队列消息失败")
	}

	return mq.parseMessages(msgStrings)
}

// PeekQueue 查看指定优先级队列中的消息，不移除
func (mq *RedisMQ) PeekQueue(ctx context.Context, priority Priority, start, stop int64) ([]*Message, error) {
	if mq.closed {
		return nil, ErrQueueClosed
	}

	if int(priority) >= mq.config.MaxPriority {
		return nil, WrapError(ErrInvalidPriority, "优先级 %d 超过最大优先级 %d", priority, mq.config.MaxPriority)
	}

	queueName := mq.getQueueNameForPriority(priority)
	msgStrings, err := mq.client.LRange(ctx, queueName, start, stop).Result()
	if err != nil {
		return nil, WrapError(err, "获取队列消息失败")
	}

	return mq.parseMessages(msgStrings)
}

// parseMessages 将字符串消息列表解析为消息对象列表
func (mq *RedisMQ) parseMessages(msgStrings []string) ([]*Message, error) {
	messages := make([]*Message, 0, len(msgStrings))

	for _, msgStr := range msgStrings {
		var msg Message
		if err := msg.UnmarshalBinary([]byte(msgStr)); err != nil {
			if mq.config.EnableLogging {
				mq.config.Logger.Warn("解析消息失败:", err)
			}
			continue
		}
		messages = append(messages, &msg)
	}

	return messages, nil
}

// RetryDeadMessages 重试死信队列中的消息，使用指定的优先级
func (mq *RedisMQ) RetryDeadMessages(ctx context.Context, count int64, priority Priority) (int64, error) {
	if mq.closed {
		return 0, ErrQueueClosed
	}

	if count <= 0 {
		return 0, nil
	}

	processed := int64(0)

	for i := int64(0); i < count; i++ {
		// 从死信队列获取消息
		message, err := mq.client.LPop(ctx, mq.deadQueue).Result()
		if err == redis.Nil {
			break // 队列为空
		}
		if err != nil {
			return processed, WrapError(err, "从死信队列获取消息失败")
		}

		// 将消息放回主队列，使用指定的优先级
		queueName := mq.getQueueNameForPriority(priority)
		err = mq.client.RPush(ctx, queueName, message).Err()
		if err != nil {
			return processed, WrapError(err, "将消息放回队列失败")
		}

		processed++
	}

	if mq.config.EnableLogging && processed > 0 {
		mq.config.Logger.Info("已将", processed, "条消息从死信队列重试, 优先级:", priority)
	}

	return processed, nil
}

// FlushQueue 清空指定优先级的队列
func (mq *RedisMQ) FlushQueue(ctx context.Context, priority Priority) error {
	if mq.closed {
		return ErrQueueClosed
	}

	if int(priority) >= mq.config.MaxPriority {
		return WrapError(ErrInvalidPriority, "优先级 %d 超过最大优先级 %d", priority, mq.config.MaxPriority)
	}

	queueName := mq.getQueueNameForPriority(priority)
	_, err := mq.client.Del(ctx, queueName).Result()
	if err != nil {
		return WrapError(err, "清空队列失败")
	}

	if mq.config.EnableLogging {
		mq.config.Logger.Info("已清空优先级队列:", priority)
	}

	return nil
}

// FlushProcessing 清空处理队列
func (mq *RedisMQ) FlushProcessing(ctx context.Context) error {
	if mq.closed {
		return ErrQueueClosed
	}

	_, err := mq.client.Del(ctx, mq.processingQueue).Result()
	if err != nil {
		return WrapError(err, "清空处理队列失败")
	}

	if mq.config.EnableLogging {
		mq.config.Logger.Info("已清空处理队列")
	}

	return nil
}

// FlushDead 清空死信队列
func (mq *RedisMQ) FlushDead(ctx context.Context) error {
	if mq.closed {
		return ErrQueueClosed
	}

	_, err := mq.client.Del(ctx, mq.deadQueue).Result()
	if err != nil {
		return WrapError(err, "清空死信队列失败")
	}

	if mq.config.EnableLogging {
		mq.config.Logger.Info("已清空死信队列")
	}

	return nil
}

// FlushAll 清空所有队列
func (mq *RedisMQ) FlushAll(ctx context.Context) error {
	if mq.closed {
		return ErrQueueClosed
	}

	pipe := mq.client.Pipeline()

	// 清空所有优先级队列
	for priority := Priority(0); int(priority) < mq.config.MaxPriority; priority++ {
		queueName := mq.getQueueNameForPriority(priority)
		pipe.Del(ctx, queueName)
	}

	// 清空处理队列和死信队列
	pipe.Del(ctx, mq.processingQueue)
	pipe.Del(ctx, mq.deadQueue)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return WrapError(err, "清空所有队列失败")
	}

	if mq.config.EnableLogging {
		mq.config.Logger.Info("已清空所有队列")
	}

	return nil
}

// IsConsumerRunning 检查指定ID的消费者是否运行中
func (mq *RedisMQ) IsConsumerRunning(consumerID string) bool {
	return mq.consumerGroup.IsConsumerRunning(consumerID)
}

// 添加监控相关方法
func (mq *RedisMQ) GetMonitor() *Monitor {
	return mq.monitor
}

// Close 关闭连接和停止所有消费者
func (mq *RedisMQ) Close() error {
	if mq.closed {
		return ErrQueueClosed
	}

	mq.closed = true

	// 停止监控器
	mq.monitor.Stop()
	// 取消上下文
	mq.cancelFunc()

	// 停止所有消费者
	if err := mq.StopAllConsumers(); err != nil {
		if mq.config.EnableLogging {
			mq.config.Logger.Error("停止所有消费者失败:", err)
		}
	}

	// 关闭Redis连接
	err := mq.client.Close()
	if err != nil {
		return WrapError(err, "关闭Redis连接失败")
	}

	if mq.config.EnableLogging {
		mq.config.Logger.Info("RedisMQ已关闭")
	}

	return nil
}

// IsClosed 检查队列是否已关闭
func (mq *RedisMQ) IsClosed() bool {
	return mq.closed
}
