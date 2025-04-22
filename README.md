# Redis Message Queue SDK for Go

这是一个基于Redis的消息队列SDK，为Go应用程序提供简单而强大的消息队列功能。

## 特性

- 支持消息优先级
- 支持多消费者并行处理
- 死信队列支持
- 优雅的消费者启动和停止
- 详细的日志记录
- 灵活的配置选项
- 消息元数据支持

## 安装

```bash
go get github.com/ghost-die/redis-mq-sdk
```
## 快速开始

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	" github.com/ghost-die/redis-mq-sdk/pkg/redismq"
)

func main() {
	// 创建上下文
	ctx := context.Background()
	
	// 创建队列实例
	mq, err := redismq.NewRedisMQ(redismq.DefaultMQConfig("my-queue"))
	if err != nil {
		log.Fatalf("创建队列失败: %v", err)
	}
	defer mq.Close()
	
	// 发布消息
	if err := mq.Publish(ctx, "Hello, Redis MQ!"); err != nil {
		log.Fatalf("发布消息失败: %v", err)
	}
	
	// 创建消息处理函数
	handler := func(msg *redismq.Message) error {
		fmt.Printf("收到消息: %s\n", msg.Content)
		return nil
	}
	
	// 启动消费者
	consumerID, err := mq.ConsumeAsync(handler)
	if err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}
	
	// 等待消息处理
	time.Sleep(time.Second)
	
	// 停止消费者
	if err := mq.StopConsumer(consumerID); err != nil {
		log.Fatalf("停止消费者失败: %v", err)
	}
}
```

## 配置选项
SDK提供了多种配置选项，可以通过MQConfig结构体进行设置：

```golang
config := redismq.DefaultMQConfig("my-queue")

// 修改Redis连接配置
config.Redis.Addr = "redis-server:6379"
config.Redis.Password = "your-password"
config.Redis.DB = 1

// 修改队列配置
config.ConsumerInterval = time.Millisecond * 200
config.MaxRetries = 5
config.EnableLogging = true

// 使用自定义日志器
config.Logger = &MyCustomLogger{}

// 创建队列实例
mq, err := redismq.NewRedisMQ(config)
```
## 优先级队列
SDK支持消息优先级，有四个预定义的优先级常量：

- redismq.HighPriority：高优先级（0）
- redismq.NormalPriority：普通优先级（5）
- redismq.LowPriority：低优先级（10）
- redismq.MinPriority：最低优先级（15）

发布带优先级的消息：

```go
// 发布高优先级消息
if err := mq.PublishWithPriority(ctx, "紧急消息", redismq.HighPriority); err != nil {
    log.Printf("发布高优先级消息失败: %v", err)
}

// 使用消息对象，可以设置优先级和元数据
msg := redismq.NewMessage("带元数据的消息", redismq.NormalPriority)
msg.SetMetadata("key1", "value1")
msg.SetMetadata("key2", 42)

if err := mq.PublishMessage(ctx, msg); err != nil {
    log.Printf("发布消息对象失败: %v", err)
}
```
消费者会按照优先级顺序处理消息，先处理高优先级的消息，然后是普通和低优先级的消息。

## 多消费者
SDK支持多个并行消费者：

```go
// 启动多个消费者
for i := 0; i < 5; i++ {
    handler := func(msg *redismq.Message) error {
        fmt.Printf("消费者 %d 收到消息: %s\n", i, msg.Content)
        return nil
    }
    
    consumerID, err := mq.ConsumeAsync(handler)
    if err != nil {
        log.Printf("启动消费者失败: %v", err)
    }
}

// 获取活跃消费者数量
count := mq.GetConsumerCount()
fmt.Printf("当前活跃消费者数量: %d\n", count)

// 停止所有消费者
if err := mq.StopAllConsumers(); err != nil {
    log.Printf("停止所有消费者失败: %v", err)
}
```

## 死信队列
当消息处理失败时，消息会被移到死信队列。您可以检查和重试这些消息：

```go
// 获取死信队列大小
deadSize, err := mq.DeadSize(ctx)
if err != nil {
    log.Printf("获取死信队列大小失败: %v", err)
}

// 查看死信消息
if deadSize > 0 {
    messages, err := mq.PeekDead(ctx, 0, deadSize-1)
    if err != nil {
        log.Printf("查看死信消息失败: %v", err)
    }
    
    for _, msg := range messages {
        fmt.Printf("死信消息: ID=%s, 内容=%s\n", msg.ID, msg.Content)
    }
    
    // 重试死信消息，使用高优先级
    retried, err := mq.RetryDeadMessages(ctx, deadSize, redismq.HighPriority)
    if err != nil {
        log.Printf("重试死信消息失败: %v", err)
    }
    fmt.Printf("已重试 %d 条死信消息\n", retried)
}
```

## 示例
查看 cmd/examples 目录中的完整示例：
- basic：基本使用示例
- priority：优先级队列示例
- consumer：多消费者示例

## 自定义日志记录
SDK支持自定义日志记录器，只需实现 Logger 接口：

```go
type Logger interface {
    Debug(args ...interface{})
    Info(args ...interface{})
    Warn(args ...interface{})
    Error(args ...interface{})
}
```
然后在配置中设置：

```go
config.Logger = myCustomLogger
```
## 许可证
MIT 许可证

```bash
现在我们可以创建一个 go.mod 文件：

```go
// go.mod

module  github.com/ghost-die/redis-mq-sdk

go 1.19

require (
	github.com/redis/go-redis/v9 v9.3.0
)
```

这样，我就创建了一个完整的基于Redis的消息队列SDK。下面总结一下SDK的主要功能和优势：

## 监控功能

SDK提供了内置的监控功能，帮助您实时了解队列的状态和性能：

```go
// 获取监控器
monitor := mq.GetMonitor()

// 设置监控间隔（默认10秒）
monitor.SetInterval(time.Second * 5)

// 添加监控钩子函数
monitor.AddHook(func(metrics map[redismq.MetricType]*redismq.MetricValue) {
    // 处理监控指标
    fmt.Printf("队列大小: %d\n", metrics[redismq.MetricQueueSize].Count)
    fmt.Printf("发布消息数: %d\n", metrics[redismq.MetricPublished].Count)
    fmt.Printf("消费消息数: %d\n", metrics[redismq.MetricConsumed].Count)
})

// 启动监控
monitor.Start()

// 获取当前指标
currentMetrics := monitor.GetMetrics()

// 停止监控
monitor.Stop()
```
监控器提供以下指标：

1. 消息指标
    - MetricPublished: 已发布消息数
    - MetricConsumed: 已消费消息数
    - MetricFailed: 处理失败消息数


2. 性能指标
    - MetricPublishTime: 发布消息耗时（平均值、最小值、最大值）
    - MetricConsumeTime: 消费消息耗时（平均值、最小值、最大值）
3. 队列状态指标
    - MetricQueueSize: 队列当前大小
    - MetricDeadSize: 死信队列当前大小

您可以通过钩子函数实时接收这些指标，并将其集成到您的监控系统中，如Prometheus、Grafana等。
查看 cmd/examples/monitor 目录中的完整监控示例。

## 监控功能总结

通过添加监控功能，您的Redis消息队列SDK现在具备了以下额外能力：

1. **实时监控**
   - 追踪消息发布/消费数量
   - 监控消息处理失败情况
   - 监控队列和死信队列大小

2. **性能监控**
   - 测量发布消息的性能（平均/最小/最大耗时）
   - 测量消费消息的性能（平均/最小/最大耗时）

3. **灵活配置**
   - 可调整监控间隔
   - 通过钩子机制支持自定义监控处理

4. **与外部监控系统集成**
   - 可以轻松集成到Prometheus、Grafana等监控系统

## SDK 主要功能

1. 消息队列基础功能
   - 消息发布和消费
   - 批量消息操作
   - 队列大小查询
   - 队列清空操作
2. 优先级队列
   - 预定义的优先级级别
   - 按优先级顺序处理消息
   - 可自定义优先级范围
3. 多消费者支持
   - 支持多个消费者并行处理消息
   - 消费者管理（启动、停止）
   - 消费者状态查询
4. 死信队列
   - 处理失败的消息自动进入死信队列
   - 查看死信队列消息
   - 重试死信消息
5. 消息格式
   - 支持字符串消息
   - 支持带元数据的消息对象
   - 消息序列化和反序列化
6. 日志和错误处理
   - 详细的日志记录
   - 自定义日志接口
   - 分类和结构化的错误类型

## 使用场景

这个消息队列SDK适用于以下场景：

1. **任务队列**：处理异步任务，如图像处理、报告生成等
2. **事件驱动架构**：在微服务之间传递事件
3. **削峰填谷**：处理流量高峰
4. **解耦**：解耦系统组件，提高可维护性
5. **工作分发**：将工作分发给多个处理者

## 设计亮点

1. **简单易用**：API设计简洁，易于理解和使用
2. **功能全面**：包含消息队列的所有基础功能和高级特性
3. **灵活配置**：可根据需求自定义配置
4. **良好的错误处理**：详细的错误信息和类型
5. **完整的示例**：多个示例展示不同使用场景
6. **优雅关闭**：支持优雅启动和关闭消费者

该SDK采用Redis作为存储后端，利用Redis的高性能和可靠性，同时提供了更高级的功能，如优先级队列和多消费者支持。它是一个轻量级的解决方案，适合中小型项目使用，同时也可以扩展到大型系统。
