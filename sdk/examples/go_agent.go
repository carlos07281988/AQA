// AQA Go Agent — 外部 Agent 接入示例
//
// 仅需 go-redis, 不依赖任何 AQA Python 代码。
// 直接读写 Redis Stream, 遵守 JSON 信封协议。
//
// 运行: go run examples/go_agent.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

// AQAMessage 是 AQA 消息信封 — 与 Python SDK 完全一致的 JSON 结构
type AQAMessage struct {
	Type          string         `json:"type"`
	MessageID     string         `json:"message_id"`
	Source        string         `json:"source"`
	Target        string         `json:"target"`
	Topic         string         `json:"topic"`
	TraceID       string         `json:"trace_id"`
	CorrelationID string         `json:"correlation_id"`
	Version       string         `json:"version"`
	Payload       map[string]any `json:"payload"`
	Timestamp     string         `json:"timestamp"`
}

func (m AQAMessage) ToJSON() string {
	b, _ := json.Marshal(m)
	return string(b)
}

func randomID() string {
	return fmt.Sprintf("%x", rand.Int63())
}

const (
	TopicProbe   = "aqa:agent:probe"
	TopicJudge   = "aqa:agent:judge"
	TopicReporter = "aqa:agent:reporter"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})

	// 创建消费组 (首次)
	rdb.XGroupCreate(ctx, TopicProbe, "go-group", "$")
	rdb.XGroupCreateMkStream(ctx, TopicProbe, "go-group", "$")

	fmt.Println("[go-agent] 已启动, 等待任务...")

	for {
		results, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "go-group",
			Consumer: "go-worker-1",
			Streams:  []string{TopicProbe, ">"},
			Count:    1,
			Block:    5 * time.Second,
		}).Result()

		if err != nil {
			continue
		}

		for _, stream := range results {
			for _, msg := range stream.Messages {
				handleMessage(ctx, rdb, msg.ID, msg.Values)
			}
		}
	}
}

func handleMessage(ctx context.Context, rdb *redis.Client, msgID string, values map[string]any) {
	raw, ok := values["json"].(string)
	if !ok {
		return
	}

	var envelope AQAMessage
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		fmt.Printf("[go-agent] 解析失败: %v\n", err)
		return
	}

	fmt.Printf("[go-agent] 收到: task_id=%v\n", envelope.Payload["task_id"])

	// 执行业务检测...
	result := AQAMessage{
		Type:          "TASK_RESULT",
		MessageID:     randomID(),
		Source:        "go-inspector",
		Target:        envelope.Source,
		Topic:         TopicJudge,
		TraceID:       envelope.TraceID,
		CorrelationID: envelope.MessageID,
		Version:       "1.0",
		Payload: map[string]any{
			"task_id": envelope.Payload["task_id"],
			"passed":  true,
			"score":   0.97,
		},
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	replyID, _ := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: TopicJudge,
		Values: map[string]any{"json": result.ToJSON()},
	}).Result()

	// ACK
	rdb.XAck(ctx, TopicProbe, "go-group", msgID)

	fmt.Printf("[go-agent] 结果已发布, msg_id=%s\n", replyID)
}
