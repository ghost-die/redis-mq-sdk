// cmd/examples/basic/main.go

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ghost-die/redis-mq-sdk/pkg/redismq"
)

// 自定义logger示例
type CustomLogger struct{}

func (l *CustomLogger) Debug(args ...interface{}) {
	fmt.Print("[DEBUG] ")
	fmt.Println(args...)
}

func (l *CustomLogger) Info(args ...interface{}) {
	fmt.Print("[INFO] ")
	fmt.Println(args...)
}

func (l *CustomLogger) Warn(args ...interface{}) {
	fmt.Print("[WARN] ")
	fmt.Println(args...)
}

func (l *CustomLogger) Error(args ...interface{}) {
	fmt.Print("[ERROR] ")
	fmt.Println(args...)
}

func main() {
	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 设置信号处理
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// 创建配置
	config := redismq.DefaultMQConfig("example-queue")

	// 使用自定义日志记录器
	config.Logger = &CustomLogger{}

	// 创建队列实例
	mq, err := redismq.NewRedisMQ(config)
	if err != nil {
		log.Fatalf("创建Redis消息队列失败: %v", err)
	}
	defer mq.Close()

	// 清空现有队列(可选)
	if err := mq.FlushAll(ctx); err != nil {
		log.Printf("清空队列失败: %v", err)
	}

	// 发布一些消息
	fmt.Println("发布消息...")
	for i := 0; i < 10; i++ {
		priority := redismq.Priority(i % 3 * 5) // 使用高、普通、低优先级
		content := fmt.Sprintf("测试消息 #%d", i)

		// 创建消息
		msg := redismq.NewMessage(content, priority)

		// 添加元数据
		msg.SetMetadata("index", i)
		msg.SetMetadata("timestamp", time.Now().Unix())

		// 发布消息
		if err := mq.PublishMessage(ctx, msg); err != nil {
			log.Printf("发布消息失败: %v", err)
		}
	}

	// 创建消息处理函数
	messageHandler := func(msg *redismq.Message) error {
		fmt.Printf("收到消息: ID=%s, 内容=%s, 优先级=%d\n", msg.ID, msg.Content, msg.Priority)

		// 获取元数据
		if index, ok := msg.GetMetadata("index").(float64); ok {
			fmt.Printf("  元数据 - 索引: %d\n", int(index))
		}

		// 随机失败一些消息(用于测试死信队列)
		if msg.Content == "测试消息 #3" {
			return fmt.Errorf("处理消息 #3 故意失败")
		}

		return nil
	}

	// 启动消费者
	fmt.Println("启动消费者...")
	consumerID, err := mq.ConsumeAsync(messageHandler)
	if err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}
	fmt.Printf("消费者已启动，ID: %s\n", consumerID)

	// 等待中断信号
	fmt.Println("服务运行中，按 Ctrl+C 停止...")
	<-signalChan
	fmt.Println("收到停止信号，准备关闭...")

	// 停止消费者
	if err := mq.StopConsumer(consumerID); err != nil {
		log.Printf("停止消费者失败: %v", err)
	}

	// 检查是否有死信消息
	deadSize, err := mq.DeadSize(ctx)
	if err != nil {
		log.Printf("获取死信队列大小失败: %v", err)
	} else if deadSize > 0 {
		fmt.Printf("死信队列中有 %d 条消息\n", deadSize)

		// 查看死信消息
		messages, err := mq.PeekDead(ctx, 0, deadSize-1)
		if err != nil {
			log.Printf("查看死信消息失败: %v", err)
		} else {
			for i, msg := range messages {
				fmt.Printf("死信消息 #%d: ID=%s, 内容=%s\n", i, msg.ID, msg.Content)
			}
		}

		// 重试死信消息
		retried, err := mq.RetryDeadMessages(ctx, deadSize, redismq.HighPriority)
		if err != nil {
			log.Printf("重试死信消息失败: %v", err)
		} else {
			fmt.Printf("已将 %d 条消息从死信队列重试(高优先级)\n", retried)
		}
	}

	fmt.Println("程序已退出")
}
