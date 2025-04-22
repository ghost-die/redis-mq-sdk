// pkg/redismq/priority.go

package redismq

// Priority 定义消息优先级
type Priority int

const (
	// HighPriority 高优先级
	HighPriority Priority = 0
	// NormalPriority 普通优先级
	NormalPriority Priority = 5
	// LowPriority 低优先级
	LowPriority Priority = 10
	// MinPriority 最低优先级
	MinPriority Priority = 15
)

// PriorityName 返回优先级的名称
func PriorityName(p Priority) string {
	switch p {
	case HighPriority:
		return "高优先级"
	case NormalPriority:
		return "普通优先级"
	case LowPriority:
		return "低优先级"
	case MinPriority:
		return "最低优先级"
	default:
		if p < NormalPriority {
			return "高优先级"
		} else if p < LowPriority {
			return "普通优先级"
		} else if p < MinPriority {
			return "低优先级"
		}
		return "最低优先级"
	}
}
