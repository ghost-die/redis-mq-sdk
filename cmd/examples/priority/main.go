// cmd/examples/priority/main.go

package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ghost-die/redis-mq-sdk/pkg/redismq"
)

func main() {
	// 创建上下文
	ctx := context.Background()

	// 创建配置
	config := redismq.DefaultMQConfig("priority-queue-demo")

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

	// 发布不同优先级的消息
	publishMessages := func() {
		// 低优先级消息
		for i := 0; i < 5; i++ {
			content := fmt.Sprintf("低优先级消息 #%d", i)
			if err := mq.PublishWithPriority(ctx, content, redismq.LowPriority); err != nil {
				log.Printf("发布低优先级消息失败: %v", err)
			}
		}

		// 普通优先级消息
		for i := 0; i < 5; i++ {
			content := fmt.Sprintf("普通优先级消息 #%d", i)
			if err := mq.PublishWithPriority(ctx, content, redismq.NormalPriority); err != nil {
				log.Printf("发布普通优先级消息失败: %v", err)
			}
		}

		// 高优先级消息
		for i := 0; i < 5; i++ {
			content := fmt.Sprintf("高优先级消息 #%d", i)
			if err := mq.PublishWithPriority(ctx, content, redismq.HighPriority); err != nil {
				log.Printf("发布高优先级消息失败: %v", err)
			}
		}
	}

	// 发布消息
	fmt.Println("发布三种优先级的消息...")
	publishMessages()

	// 获取各优先级队列大小
	highSize, _ := mq.SizeByPriority(ctx, redismq.HighPriority)
	normalSize, _ := mq.SizeByPriority(ctx, redismq.NormalPriority)
	lowSize, _ := mq.SizeByPriority(ctx, redismq.LowPriority)

	fmt.Printf("队列大小 - 高优先级: %d, 普通优先级: %d, 低优先级: %d\n",
		highSize, normalSize, lowSize)

	// 创建消息处理函数
	messageHandler := func(msg *redismq.Message) error {
		var priorityName string
		switch msg.Priority {
		case redismq.HighPriority:
			priorityName = "高"
		case redismq.NormalPriority:
			priorityName = "普通"
		case redismq.LowPriority:
			priorityName = "低"
		default:
			priorityName = fmt.Sprintf("未知(%d)", msg.Priority)
		}

		fmt.Printf("收到%s消息: %s\n", priorityName, msg.Content)

		// 模拟处理时间
		time.Sleep(time.Millisecond * 200)

		return nil
	}

	// 启动消费者
	fmt.Println("启动消费者，将按优先级顺序处理消息...")
	consumerID, err := mq.ConsumeAsync(messageHandler)
	if err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}

	// 等待所有消息被处理
	time.Sleep(time.Second * 5)

	// 停止消费者
	if err := mq.StopConsumer(consumerID); err != nil {
		log.Printf("停止消费者失败: %v", err)
	}

	// 检查队列是否为空
	totalSize, _ := mq.Size(ctx)
	fmt.Printf("所有队列中剩余消息: %d\n", totalSize)

	// 如果还有消息，继续发布更多消息，测试优先级是否正常工作
	if totalSize == 0 {
		// 再次发布消息，但这次延迟启动消费者，并在消费过程中发布高优先级消息
		fmt.Println("\n再次测试，先发布低优先级消息，后发布高优先级消息...")

		// 先发布低优先级消息
		for i := 0; i < 10; i++ {
			content := fmt.Sprintf("低优先级消息(第二批) #%d", i)
			if err := mq.PublishWithPriority(ctx, content, redismq.LowPriority); err != nil {
				log.Printf("发布低优先级消息失败: %v", err)
			}
		}

		// 启动消费者
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()

			// 启动消费者
			consumerID, err := mq.ConsumeAsync(messageHandler)
			if err != nil {
				log.Printf("启动消费者失败: %v", err)
				return
			}

			// 等待消费者处理一些消息
			time.Sleep(time.Second)

			// 在消费过程中发布高优先级消息
			fmt.Println("在处理低优先级消息过程中，发布高优先级消息...")
			for i := 0; i < 5; i++ {
				content := fmt.Sprintf("高优先级消息(插队) #%d", i)
				if err := mq.PublishWithPriority(ctx, content, redismq.HighPriority); err != nil {
					log.Printf("发布高优先级消息失败: %v", err)
				}
			}

			// 等待所有消息处理完成
			time.Sleep(time.Second * 5)

			// 停止消费者
			if err := mq.StopConsumer(consumerID); err != nil {
				log.Printf("停止消费者失败: %v", err)
			}
		}()

		// 等待测试完成
		wg.Wait()
	}

	fmt.Println("优先级队列演示完成")
}
