/**
 * AQA SDK — JavaScript/Node.js 外部 Agent 接入示例
 *
 * 仅需 redis (node-redis), 不依赖 AQA Python 包。
 * 直接读写 Redis Stream, 遵守 JSON 信封协议即可。
 *
 * 运行: node examples/js_agent.mjs
 */
import { createClient } from "redis";

const REDIS_URL = "redis://127.0.0.1:***@ {message_id}
        );

        // 处理任务
        const result = {
            task_id: msg.payload.task_id,
            passed: true,
            score: 0.94,
            checked_by: "js-validator",
        };

        // 发布结果到 judge topic
        const reply = {
            type: "TASK_RESULT",
            message_id: randomId(),
            source: "js-validator",
            target: msg.source,
            topic: "aqa:agent:judge",
            trace_id: msg.trace_id,
            correlation_id: msg.message_id,
            version: "1.0",
            payload: result,
            timestamp: new Date().toISOString(),
        };

        const replyId = await publisher.xAdd(
            "aqa:agent:judge",
            "*",
            { json: JSON.stringify(reply) },
            { TRIM: { strategy: "MAXLEN", threshold: 10000, strategyModifier: "~" } }
        );
        console.log(`[js-agent] 结果已发布, msg_id=${replyId}`);
    } catch (err) {
        console.error(`[js-agent] 处理失败:`, err.message);
    }
}

// 创建消费者组 (首次需要)
try {
    await subscriber.xGroupCreate("aqa:agent:probe", "js-group", "$", {
        MKSTREAM: true,
    });
} catch (e) {
    // 组已存在
}

// 消费循环
console.log("[js-agent] 已启动, 等待任务...");
while (true) {
    const results = await subscriber.xReadGroup(
        "js-group",
        "js-worker-1",
        { "aqa:agent:probe": ">" },
        { COUNT: 1, BLOCK: 5000 }
    );

    if (results) {
        for (const { name, messages } of results) {
            for (const msg of messages) {
                await handleMessage(
                    msg.message.message_id,
                    JSON.parse(msg.message.json)
                );
            }
        }
    }
}
