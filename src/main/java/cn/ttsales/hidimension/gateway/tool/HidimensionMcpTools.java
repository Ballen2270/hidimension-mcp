package cn.ttsales.hidimension.gateway.tool;

import cn.ttsales.hidimension.gateway.client.HidimensionClient;
import cn.ttsales.hidimension.gateway.config.HidimensionProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * 工具实现：接收 MCP 客户端 JSON 输入，编排 create(含 cookie 缓存/失效重试) → poll → result 全链路。
 * 每个工具方法返回 {@code Mono<String>}（JSON），供 {@code ToolCallback.call()} 内部 block。
 */
@Slf4j
@Component
public class HidimensionMcpTools {

    private final HidimensionClient client;
    private final HidimensionProperties props;
    private final ObjectMapper objectMapper;

    public HidimensionMcpTools(HidimensionClient client, HidimensionProperties props, ObjectMapper objectMapper) {
        this.client = client;
        this.props = props;
        this.objectMapper = objectMapper;
    }

    // ──────────────── 工具1: 蛋白质结构预测 ────────────────

    public Mono<String> runProteinStructurePredict(String inputJson, String email, String password) {
        return Mono.defer(() -> {
            JsonNode input;
            try {
                input = objectMapper.readTree(inputJson);
            } catch (JsonProcessingException e) {
                return Mono.just(errorJson("Invalid JSON input: " + e.getMessage()));
            }

            ObjectNode requestBody = objectMapper.createObjectNode();
            int numStructures = input.has("numStructures") ? input.get("numStructures").asInt() : 1;
            requestBody.put("numStructures", numStructures);
            if (!input.has("seqList") || !input.get("seqList").isArray()) {
                return Mono.just(errorJson("Missing or invalid 'seqList' field — required array of [{header, seq}]"));
            }
            requestBody.set("seqList", input.get("seqList"));

            String requestBodyStr;
            try {
                requestBodyStr = objectMapper.writeValueAsString(requestBody);
            } catch (JsonProcessingException e) {
                return Mono.just(errorJson("Failed to serialize request: " + e.getMessage()));
            }

            log.info("Protein structure predict V2: numStructures={}, seqCount={}", numStructures, input.get("seqList").size());

            return client.createProteinStructurePredict(email, password, requestBodyStr)
                    .flatMap(created -> chainResult(created))
                    .onErrorResume(e -> {
                        log.error("Protein structure predict V2 failed", e);
                        return Mono.just(errorJson(e.getMessage()));
                    });
        });
    }

    // ──────────────── 工具2: 结构同源搜索 ────────────────

    public Mono<String> runStructEmbeddingSearchPro(String inputJson, String email, String password) {
        return Mono.defer(() -> {
            JsonNode input;
            try {
                input = objectMapper.readTree(inputJson);
            } catch (JsonProcessingException e) {
                return Mono.just(errorJson("Invalid JSON input: " + e.getMessage()));
            }

            if (!input.has("db") || !input.get("db").isArray() || input.get("db").size() == 0) {
                return Mono.just(errorJson("Missing or invalid 'db' field — required non-empty array of database names"));
            }
            if (!input.has("seqList") || !input.get("seqList").isArray()) {
                return Mono.just(errorJson("Missing or invalid 'seqList' field — required array of [{header, seq}]"));
            }

            ObjectNode requestBody = objectMapper.createObjectNode();
            requestBody.set("db", input.get("db"));
            int count = input.path("count").asInt(1000);
            double radius = input.path("radius").asDouble(0.0);
            boolean doAligner = input.path("doAligner").asBoolean(true);
            boolean doLink = input.path("doLink").asBoolean(true);
            requestBody.put("count", count);
            requestBody.put("radius", radius);
            requestBody.put("doAligner", doAligner);
            requestBody.put("doLink", doLink);
            requestBody.set("seqList", input.get("seqList"));

            String requestBodyStr;
            try {
                requestBodyStr = objectMapper.writeValueAsString(requestBody);
            } catch (JsonProcessingException e) {
                return Mono.just(errorJson("Failed to serialize request: " + e.getMessage()));
            }

            log.info("Struct embedding search pro: db={}, count={}, radius={}, seqCount={}",
                    input.get("db"), count, radius, input.get("seqList").size());

            return client.createStructEmbeddingSearchPro(email, password, requestBodyStr)
                    .flatMap(created -> chainResult(created))
                    .onErrorResume(e -> {
                        log.error("Struct embedding search pro failed", e);
                        return Mono.just(errorJson(e.getMessage()));
                    });
        });
    }

    // ──────────────── helper ────────────────

    /** 从 create 结果取 id(轮询)/res(取结果)，轮询并序列化为 JSON */
    private Mono<String> chainResult(HidimensionClient.CreatedTask created) {
        JsonNode info = created.info();
        String userTaskId = info.has("id") ? info.get("id").asText() : "";
        String taskId = info.has("res") ? info.get("res").asText() : "";
        if (userTaskId.isEmpty() || taskId.isEmpty()) {
            return Mono.just(errorJson("Task created but no id/res in response"));
        }
        log.info("Task created: userTaskId={}, taskId={}", userTaskId, taskId);
        return client.pollThenResult(created.cookie(), created.taskPath(), userTaskId, taskId,
                        props.getPolling().getInterval(), props.getPolling().getTimeout())
                .map(result -> {
                    try {
                        return objectMapper.writeValueAsString(result);
                    } catch (JsonProcessingException e) {
                        return errorJson("Failed to serialize result: " + e.getMessage());
                    }
                });
    }

    private String errorJson(String message) {
        try {
            return objectMapper.writeValueAsString(Map.of("error", message == null ? "" : message));
        } catch (JsonProcessingException e) {
            return "{\"error\":\"error serialization failed\"}";
        }
    }
}
