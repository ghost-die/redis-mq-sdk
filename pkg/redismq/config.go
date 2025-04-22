// pkg/redismq/config.go

package redismq

import "time"

// RedisConfig 存储Redis连接配置
type RedisConfig struct {
	// Addr Redis地址，格式为"host:port"
	Addr string
	// Password Redis密码
	Password string
	// DB Redis数据库索引
	DB int
	// MaxRetries 最大重试次数
	MaxRetries int
	// MinRetryBackoff 最小重试间隔
	MinRetryBackoff time.Duration
	// MaxRetryBackoff 最大重试间隔
	MaxRetryBackoff time.Duration
	// DialTimeout 连接超时
	DialTimeout time.Duration
	// ReadTimeout 读取超时
	ReadTimeout time.Duration
	// WriteTimeout 写入超时
	WriteTimeout time.Duration
}

// MQConfig 消息队列配置
type MQConfig struct {
	// Redis连接配置
	Redis RedisConfig
	// QueueName 队列名称
	QueueName string
	// ConsumerTimeout 消费者超时时间
	ConsumerTimeout time.Duration
	// ConsumerInterval 消费者轮询间隔
	ConsumerInterval time.Duration
	// MaxRetries 处理消息时出错后的重试次数
	MaxRetries int
	// AckDeadline 消息确认时间
	AckDeadline time.Duration
	// MaxPriority 优先级数量 (从0到MaxPriority-1)
	MaxPriority int
	// DefaultPriority 默认优先级
	DefaultPriority Priority
	// EnableLogging 是否启用日志
	EnableLogging bool
	// Logger 日志接口
	Logger Logger
}

// DefaultRedisConfig 返回默认Redis配置
func DefaultRedisConfig() RedisConfig {
	return RedisConfig{
		Addr:            "localhost:6379",
		Password:        "",
		DB:              0,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond * 8,
		MaxRetryBackoff: time.Millisecond * 512,
		DialTimeout:     time.Second * 5,
		ReadTimeout:     time.Second * 3,
		WriteTimeout:    time.Second * 3,
	}
}

// DefaultMQConfig 返回默认队列配置
func DefaultMQConfig(queueName string) MQConfig {
	return MQConfig{
		Redis:            DefaultRedisConfig(),
		QueueName:        queueName,
		ConsumerTimeout:  time.Second * 5,
		ConsumerInterval: time.Millisecond * 100,
		MaxRetries:       3,
		AckDeadline:      time.Minute * 5,
		MaxPriority:      16,
		DefaultPriority:  NormalPriority,
		EnableLogging:    true,
		Logger:           &defaultLogger{},
	}
}

// Logger 定义日志接口
type Logger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
}

// defaultLogger 默认日志实现
type defaultLogger struct{}

func (l *defaultLogger) Debug(args ...interface{}) {}
func (l *defaultLogger) Info(args ...interface{})  {}
func (l *defaultLogger) Warn(args ...interface{})  {}
func (l *defaultLogger) Error(args ...interface{}) {}
