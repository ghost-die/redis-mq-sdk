// pkg/redismq/monitor.go

package redismq

import (
	"context"
	"sync"
	"time"
)

// MetricType 定义监控指标类型
type MetricType string

const (
	// 消息指标
	MetricPublished MetricType = "published" // 已发布消息数
	MetricConsumed  MetricType = "consumed"  // 已消费消息数
	MetricFailed    MetricType = "failed"    // 处理失败消息数

	// 性能指标
	MetricPublishTime MetricType = "publish_time" // 发布消息耗时
	MetricConsumeTime MetricType = "consume_time" // 消费消息耗时

	// 队列状态指标
	MetricQueueSize MetricType = "queue_size" // 队列大小
	MetricDeadSize  MetricType = "dead_size"  // 死信队列大小
)

// MetricValue 定义监控指标值
type MetricValue struct {
	Count    int64   // 计数
	Duration int64   // 总持续时间（纳秒）
	Average  float64 // 平均值
	Min      int64   // 最小值
	Max      int64   // 最大值
}

// Metrics 定义监控指标集合
type Metrics struct {
	values map[MetricType]*MetricValue
	mutex  sync.RWMutex
}

// Monitor 监控管理器
type Monitor struct {
	mq        *RedisMQ
	metrics   *Metrics
	stopChan  chan struct{}
	interval  time.Duration
	isRunning bool
	mutex     sync.Mutex
	hooks     []MonitorHook
}

// MonitorHook 监控钩子函数，用于响应监控事件
type MonitorHook func(metrics map[MetricType]*MetricValue)

// newMonitor 创建新的监控器
func newMonitor(mq *RedisMQ) *Monitor {
	return &Monitor{
		mq:       mq,
		metrics:  &Metrics{values: make(map[MetricType]*MetricValue)},
		stopChan: make(chan struct{}),
		interval: time.Second * 10, // 默认监控间隔
		hooks:    make([]MonitorHook, 0),
	}
}

// AddHook 添加监控钩子
func (m *Monitor) AddHook(hook MonitorHook) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.hooks = append(m.hooks, hook)
}

// SetInterval 设置监控间隔
func (m *Monitor) SetInterval(interval time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.interval = interval
}

// Start 启动监控
func (m *Monitor) Start() {
	m.mutex.Lock()
	if m.isRunning {
		m.mutex.Unlock()
		return
	}
	m.isRunning = true
	m.mutex.Unlock()

	// 初始化所有指标类型
	m.metrics.mutex.Lock()
	for _, metricType := range []MetricType{
		MetricPublished, MetricConsumed, MetricFailed,
		MetricPublishTime, MetricConsumeTime,
		MetricQueueSize, MetricDeadSize,
	} {
		m.metrics.values[metricType] = &MetricValue{}
	}
	m.metrics.mutex.Unlock()

	// 启动监控任务
	go m.monitorTask()
}

// Stop 停止监控
func (m *Monitor) Stop() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.isRunning {
		return
	}

	close(m.stopChan)
	m.isRunning = false
}

// monitorTask 监控任务
func (m *Monitor) monitorTask() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			// 更新队列状态指标
			m.updateQueueMetrics()

			// 触发钩子函数
			m.triggerHooks()
		}
	}
}

// updateQueueMetrics 更新队列指标
func (m *Monitor) updateQueueMetrics() {
	ctx := context.Background()

	// 获取队列大小
	size, err := m.mq.Size(ctx)
	if err == nil {
		m.metrics.mutex.Lock()
		m.metrics.values[MetricQueueSize].Count = size
		m.metrics.mutex.Unlock()
	}

	// 获取死信队列大小
	deadSize, err := m.mq.DeadSize(ctx)
	if err == nil {
		m.metrics.mutex.Lock()
		m.metrics.values[MetricDeadSize].Count = deadSize
		m.metrics.mutex.Unlock()
	}
}

// triggerHooks 触发钩子函数
func (m *Monitor) triggerHooks() {
	if len(m.hooks) == 0 {
		return
	}

	// 创建指标快照
	m.metrics.mutex.RLock()
	snapshot := make(map[MetricType]*MetricValue)
	for k, v := range m.metrics.values {
		metric := &MetricValue{
			Count:    v.Count,
			Duration: v.Duration,
			Average:  v.Average,
			Min:      v.Min,
			Max:      v.Max,
		}
		snapshot[k] = metric
	}
	m.metrics.mutex.RUnlock()

	// 触发钩子
	for _, hook := range m.hooks {
		hook(snapshot)
	}
}

// RecordPublish 记录发布消息
func (m *Monitor) RecordPublish(duration time.Duration) {
	m.recordMetric(MetricPublished, 1, duration)
	m.recordDuration(MetricPublishTime, duration)
}

// RecordConsume 记录消费消息
func (m *Monitor) RecordConsume(duration time.Duration) {
	m.recordMetric(MetricConsumed, 1, 0)
	m.recordDuration(MetricConsumeTime, duration)
}

// RecordFailed 记录消息处理失败
func (m *Monitor) RecordFailed() {
	m.recordMetric(MetricFailed, 1, 0)
}

// recordMetric 记录通用指标
func (m *Monitor) recordMetric(metricType MetricType, count int64, duration time.Duration) {
	m.metrics.mutex.Lock()
	defer m.metrics.mutex.Unlock()

	if metric, ok := m.metrics.values[metricType]; ok {
		metric.Count += count
		metric.Duration += duration.Nanoseconds()
		if metric.Count > 0 {
			metric.Average = float64(metric.Duration) / float64(metric.Count)
		}
	}
}

// recordDuration 记录持续时间
func (m *Monitor) recordDuration(metricType MetricType, duration time.Duration) {
	durationNs := duration.Nanoseconds()

	m.metrics.mutex.Lock()
	defer m.metrics.mutex.Unlock()

	if metric, ok := m.metrics.values[metricType]; ok {
		// 更新最小值
		if metric.Min == 0 || durationNs < metric.Min {
			metric.Min = durationNs
		}

		// 更新最大值
		if durationNs > metric.Max {
			metric.Max = durationNs
		}
	}
}

// GetMetrics 获取所有指标
func (m *Monitor) GetMetrics() map[MetricType]*MetricValue {
	m.metrics.mutex.RLock()
	defer m.metrics.mutex.RUnlock()

	// 创建副本
	metrics := make(map[MetricType]*MetricValue)
	for k, v := range m.metrics.values {
		metrics[k] = &MetricValue{
			Count:    v.Count,
			Duration: v.Duration,
			Average:  v.Average,
			Min:      v.Min,
			Max:      v.Max,
		}
	}

	return metrics
}

// IsRunning 检查监控是否运行中
func (m *Monitor) IsRunning() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.isRunning
}
