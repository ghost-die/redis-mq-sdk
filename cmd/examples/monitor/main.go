// cmd/examples/monitor/main.go

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

// 打印指标
func printMetrics(metrics map[redismq.MetricType]*redismq.MetricValue) {
	fmt.Println("\n--------- 队列监控指标 ---------")

	// 消息指标
	fmt.Printf("已发布消息: %d\n", metrics[redismq.MetricPublished].Count)
	fmt.Printf("已消费消息: %d\n", metrics[redismq.MetricConsumed].Count)
	fmt.Printf("处理失败消息: %d\n", metrics[redismq.MetricFailed].Count)

	// 性能指标
	if metrics[redismq.MetricPublishTime].Count > 0 {
		fmt.Printf("发布消息平均耗时: %.2f ms\n", float64(metrics[redismq.MetricPublishTime].Average)/float64(time.Millisecond))
		fmt.Printf("发布消息最小耗时: %.2f ms\n", float64(metrics[redismq.MetricPublishTime].Min)/float64(time.Millisecond))
		fmt.Printf("发布消息最大耗时: %.2f ms\n", float64(metrics[redismq.MetricPublishTime].Max)/float64(time.Millisecond))
	}

	if metrics[redismq.MetricConsumeTime].Count > 0 {
		fmt.Printf("消费消息平均耗时: %.2f ms\n", float64(metrics[redismq.MetricConsumeTime].Average)/float64(time.Millisecond))
		fmt.Printf("消费消息最小耗时: %.2f ms\n", float64(metrics[redismq.MetricConsumeTime].Min)/float64(time.Millisecond))
		fmt.Printf("消费消息最大耗时: %.2f ms\n", float64(metrics[redismq.MetricConsumeTime].Max)/float64(time.Millisecond))
	}

	// 队列状态指标
	fmt.Printf("队列中消息数: %d\n", metrics[redismq.MetricQueueSize].Count)
	fmt.Printf("死信队列消息数: %d\n", metrics[redismq.MetricDeadSize].Count)

	fmt.Println("--------------------------------")
}

func main() {
	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 设置信号处理
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// 创建配置
	config := redismq.DefaultMQConfig("monitor-demo")

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

	// 获取监控器
	monitor := mq.GetMonitor()

	// 设置监控间隔
	monitor.SetInterval(time.Second * 5)

	// 添加监控钩子
	monitor.AddHook(printMetrics)

	// 启动监控
	monitor.Start()
	fmt.Println("监控已启动...")

	// 创建消息处理函数
	messageHandler := func(msg *redismq.Message) error {
		// 模拟处理时间 50-150ms
		processingTime := time.Duration(50+time.Now().UnixNano()%100) * time.Millisecond
		time.Sleep(processingTime)

		// 随机失败一些消息
		if time.Now().UnixNano()%10 == 0 {
			return fmt.Errorf("模拟随机处理失败")
		}

		return nil
	}

	// 启动消费者
	consumerID, err := mq.ConsumeAsync(messageHandler)
	if err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}
	fmt.Printf("消费者已启动，ID: %s\n", consumerID)

	// 在一个单独的协程中定期发布消息
	go func() {
		messageCount := 0
		ticker := time.NewTicker(time.Millisecond * 200)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// 发布消息
				msg := redismq.NewMessage(
					fmt.Sprintf("监控测试消息 #%d", messageCount),
					redismq.Priority(messageCount%15), // 使用不同优先级
				)

				if err := mq.PublishMessage(ctx, msg); err != nil {
					log.Printf("发布消息失败: %v", err)
				}

				messageCount++
			}
		}
	}()

	// 等待中断信号
	fmt.Println("服务运行中，按 Ctrl+C 停止...")
	<-signalChan
	fmt.Println("收到停止信号，准备关闭...")

	// 取消上下文，停止发布消息
	cancel()

	// 停止消费者
	if err := mq.StopConsumer(consumerID); err != nil {
		log.Printf("停止消费者失败: %v", err)
	}

	// 停止监控器
	monitor.Stop()

	// 获取最终的监控指标
	finalMetrics := monitor.GetMetrics()
	fmt.Println("最终监控数据:")
	printMetrics(finalMetrics)

	fmt.Println("程序已退出")
}
