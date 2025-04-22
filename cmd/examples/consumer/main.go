// cmd/examples/consumer/main.go

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/ghost-die/redis-mq-sdk/pkg/redismq"
)

func main() {
	// 创建上下文
	ctx := context.Background()

	// 创建配置
	config := redismq.DefaultMQConfig("multi-consumer-demo")

	// 创建队列实例
	mq, err := redismq.NewRedisMQ(config)
	if err != nil {
		log.Fatalf("创建Redis消息队列失败: %v", err)
	}
	defer mq.Close()

	// 清空现有队列
	if err := mq.FlushAll(ctx); err != nil {
		log.Printf("清空队列失败: %v", err)
	}

	// 准备充足的消息
	messageCount := 100
	fmt.Printf("发布 %d 条消息...\n", messageCount)

	messages := make([]string, messageCount)
	for i := 0; i < messageCount; i++ {
		messages[i] = fmt.Sprintf("批量消息 #%d", i)
	}

	if err := mq.PublishBatch(ctx, messages); err != nil {
		log.Fatalf("批量发布消息失败: %v", err)
	}

	// 获取队列大小
	size, _ := mq.Size(ctx)
	fmt.Printf("队列大小: %d\n", size)

	// 创建计数器
	var processedCount int32

	// 创建多个消费者处理消息
	consumerCount := 5
	consumerIDs := make([]string, consumerCount)

	// 启动消费者
	for i := 0; i < consumerCount; i++ {
		consumerIndex := i

		// 创建消息处理函数，每个消费者有不同的处理时间
		messageHandler := func(msg *redismq.Message) error {
			// 更新计数器
			count := atomic.AddInt32(&processedCount, 1)

			// 消费者编号
			consumerName := fmt.Sprintf("消费者-%d", consumerIndex)

			// 打印消息信息
			fmt.Printf("[%s] 处理消息: %s (进度: %d/%d)\n",
				consumerName, msg.Content, count, messageCount)

			// 模拟不同的处理时间
			processingTime := time.Duration(50+rand.Intn(150)) * time.Millisecond
			time.Sleep(processingTime)

			return nil
		}

		// 启动消费者
		consumerID, err := mq.ConsumeAsync(messageHandler)
		if err != nil {
			log.Printf("启动消费者 %d 失败: %v", consumerIndex, err)
			continue
		}

		consumerIDs[consumerIndex] = consumerID
		fmt.Printf("消费者 %d 已启动，ID: %s\n", consumerIndex, consumerID)
	}

	// 显示活跃消费者数量
	fmt.Printf("活跃消费者数量: %d\n", mq.GetConsumerCount())

	// 等待所有消息处理完成或超时
	timeout := time.After(time.Second * 30)
	checkInterval := time.NewTicker(time.Second)

	for {
		select {
		case <-timeout:
			fmt.Println("处理超时")
			goto cleanup
		case <-checkInterval.C:
			// 检查队列大小
			size, _ := mq.Size(ctx)
			processed := atomic.LoadInt32(&processedCount)

			fmt.Printf("进度: %d/%d, 队列中剩余: %d\n", processed, messageCount, size)

			// 所有消息都已处理
			if processed >= int32(messageCount) {
				fmt.Println("所有消息已处理完成")
				goto cleanup
			}

			// 如果处理了一半消息，停止一部分消费者
			if processed >= int32(messageCount)/2 && len(consumerIDs) > 2 {
				fmt.Println("处理了一半消息，减少消费者数量...")

				// 停止部分消费者
				for i := 0; i < 2; i++ {
					if i < len(consumerIDs) {
						consumerID := consumerIDs[i]
						if mq.IsConsumerRunning(consumerID) {
							fmt.Printf("停止消费者 ID: %s\n", consumerID)
							if err := mq.StopConsumer(consumerID); err != nil {
								log.Printf("停止消费者失败: %v", err)
							}
						}
					}
				}

				// 更新消费者ID列表
				consumerIDs = consumerIDs[2:]

				// 显示活跃消费者数量
				fmt.Printf("活跃消费者数量: %d\n", mq.GetConsumerCount())
			}
		}
	}

cleanup:
	// 停止所有消费者
	fmt.Println("停止所有消费者...")
	if err := mq.StopAllConsumers(); err != nil {
		log.Printf("停止所有消费者失败: %v", err)
	}

	// 完成统计
	fmt.Printf("多消费者演示完成，共处理 %d 条消息\n", atomic.LoadInt32(&processedCount))
}
