// pkg/redismq/message.go

package redismq

import (
	"encoding/json"
	"time"
)

// Message 表示一个消息
type Message struct {
	// ID 消息唯一标识
	ID string `json:"id"`
	// Content 消息内容
	Content string `json:"content"`
	// Priority 消息优先级
	Priority Priority `json:"priority"`
	// Timestamp 创建时间戳
	Timestamp int64 `json:"timestamp"`
	// Metadata 元数据，用户可以存放自定义数据
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// NewMessage 创建一个新的消息
func NewMessage(content string, priority Priority) *Message {
	return &Message{
		ID:        generateID(),
		Content:   content,
		Priority:  priority,
		Timestamp: time.Now().UnixNano(),
		Metadata:  make(map[string]interface{}),
	}
}

// SetMetadata 设置元数据
func (m *Message) SetMetadata(key string, value interface{}) *Message {
	if m.Metadata == nil {
		m.Metadata = make(map[string]interface{})
	}
	m.Metadata[key] = value
	return m
}

// GetMetadata 获取元数据
func (m *Message) GetMetadata(key string) interface{} {
	if m.Metadata == nil {
		return nil
	}
	return m.Metadata[key]
}

// MarshalBinary 实现 encoding.BinaryMarshaler 接口
func (m *Message) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

// UnmarshalBinary 实现 encoding.BinaryUnmarshaler 接口
func (m *Message) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
}

// generateID 生成唯一ID
func generateID() string {
	return time.Now().Format("20060102150405.000000000") + "-" + randomString(8)
}

// randomString 生成指定长度的随机字符串
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
		time.Sleep(time.Nanosecond)
	}
	return string(b)
}
