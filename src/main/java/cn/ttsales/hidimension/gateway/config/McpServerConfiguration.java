package cn.ttsales.hidimension.gateway.config;

import cn.ttsales.hidimension.gateway.tool.HidimensionMcpTools;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.server.McpTransportContextExtractor;
import io.modelcontextprotocol.server.transport.WebFluxStreamableServerTransportProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.mcp.McpToolUtils;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.definition.DefaultToolDefinition;
import org.springframework.ai.tool.definition.ToolDefinition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.server.ServerRequest;

import java.util.Base64;
import java.util.Map;

/**
 * MCP 服务配置：
 * 1. 自定义 WebFluxStreamableServerTransportProvider —— contextExtractor 解码 HTTP Basic Auth，
 *    把 email/password 写入 McpTransportContext（不使用 ThreadLocal，避免跨线程丢失）。
 * 2. ToolCallbackProvider —— 编程式注册 2 个工具，description 来自 application.yml。
 * <p>
 * 凭据透传链：contextExtractor(写 transport context) → MCP server 调 call(json, ToolContext)
 * （ToolContext 带 exchange，见 McpToolUtils#toSharedSyncToolSpecification）→
 * 工具从 McpToolUtils.getMcpExchange(toolContext).transportContext() 取 email/password。
 * 注解扫描已关闭（spring.ai.mcp.server.annotation-scanner.enabled=false），工具完全由 ToolCallbackProvider 提供。
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(HidimensionProperties.class)
public class McpServerConfiguration {

    private static final String TRANSPORT_KEY_EMAIL = "email";
    private static final String TRANSPORT_KEY_PASSWORD = "password";

    private final ObjectMapper objectMapper;

    public McpServerConfiguration(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // ──────────── Transport Provider ────────────

    /**
     * 自定义 WebFlux Streamable HTTP transport provider（覆盖 auto-config 默认 bean）。
     * 解码 Authorization: Basic base64(email:password)，按第一个 ':' 拆分（RFC 7617 允许密码含 ':'），
     * 写入 McpTransportContext，供工具经 exchange 取用。
     */
    @Bean
    public WebFluxStreamableServerTransportProvider webFluxStreamableServerTransportProvider() {
        return WebFluxStreamableServerTransportProvider.builder()
                .messageEndpoint("/mcp")
                .contextExtractor((McpTransportContextExtractor<ServerRequest>) request -> {
                    Credentials cred = decodeBasic(request.headers().firstHeader("Authorization"));
                    if (cred != null) {
                        return McpTransportContext.create(Map.of(
                                TRANSPORT_KEY_EMAIL, cred.email(),
                                TRANSPORT_KEY_PASSWORD, cred.password()));
                    }
                    return McpTransportContext.create(Map.of());
                })
                .build();
    }

    // ──────────── ToolCallbackProvider ────────────

    /**
     * 编程式注册 2 个 MCP 工具，description 从 HidimensionProperties 读取。
     * call(String, ToolContext) 由 MCP server 调用（见 McpToolUtils），ToolContext 内含 MCP exchange；
     * 工具跑在 boundedElastic，故内部 Mono.block() 不会阻塞 event loop。
     */
    @Bean
    public ToolCallbackProvider hidimensionToolCallbackProvider(HidimensionMcpTools tools,
                                                                HidimensionProperties props) {
        return () -> new ToolCallback[]{
                createProteinStructurePredictCallback(tools, props),
                createStructEmbeddingSearchProCallback(tools, props)
        };
    }

    private ToolCallback createProteinStructurePredictCallback(HidimensionMcpTools tools,
                                                               HidimensionProperties props) {
        String description = props.getTools().getProteinStructurePredict().getDescription();
        return new ToolCallback() {
            @Override
            public ToolDefinition getToolDefinition() {
                return DefaultToolDefinition.builder()
                        .name("run_protein_structure_predict")
                        .description(description)
                        .inputSchema(INPUT_SCHEMA_PROTEIN_STRUCTURE_PREDICT)
                        .build();
            }

            @Override
            public String call(String toolInput) {
                return call(toolInput, null); // 兜底；MCP server 实际走 2 参版
            }

            @Override
            public String call(String toolInput, ToolContext toolContext) {
                Credentials cred = extractCredentials(toolContext);
                if (cred == null) {
                    return errorJson("Authentication required: provide HTTP Basic Auth header (base64(email:password))");
                }
                try {
                    return tools.runProteinStructurePredict(toolInput, cred.email(), cred.password()).block();
                } catch (Exception e) {
                    log.error("run_protein_structure_predict execution error", e);
                    return errorJson(e.getMessage());
                }
            }
        };
    }

    private ToolCallback createStructEmbeddingSearchProCallback(HidimensionMcpTools tools,
                                                                HidimensionProperties props) {
        String description = props.getTools().getStructEmbeddingSearchPro().getDescription();
        return new ToolCallback() {
            @Override
            public ToolDefinition getToolDefinition() {
                return DefaultToolDefinition.builder()
                        .name("run_struct_embedding_search_pro")
                        .description(description)
                        .inputSchema(INPUT_SCHEMA_STRUCT_EMBEDDING_SEARCH_PRO)
                        .build();
            }

            @Override
            public String call(String toolInput) {
                return call(toolInput, null);
            }

            @Override
            public String call(String toolInput, ToolContext toolContext) {
                Credentials cred = extractCredentials(toolContext);
                if (cred == null) {
                    return errorJson("Authentication required: provide HTTP Basic Auth header (base64(email:password))");
                }
                try {
                    return tools.runStructEmbeddingSearchPro(toolInput, cred.email(), cred.password()).block();
                } catch (Exception e) {
                    log.error("run_struct_embedding_search_pro execution error", e);
                    return errorJson(e.getMessage());
                }
            }
        };
    }

    // ──────────── helpers ────────────

    /** 从 ToolContext 取 MCP exchange，再取 transport context 里的 Basic 凭据 */
    private static Credentials extractCredentials(ToolContext toolContext) {
        return McpToolUtils.getMcpExchange(toolContext)
                .map(McpSyncServerExchange::transportContext)
                .map(tctx -> {
                    Object email = tctx.get(TRANSPORT_KEY_EMAIL);
                    Object password = tctx.get(TRANSPORT_KEY_PASSWORD);
                    if (email instanceof String e && password instanceof String p) {
                        return new Credentials(e, p);
                    }
                    return null;
                })
                .orElse(null);
    }

    private static Credentials decodeBasic(String authHeader) {
        if (!StringUtils.hasText(authHeader) || !authHeader.startsWith("Basic ")) {
            return null;
        }
        try {
            String decoded = new String(Base64.getDecoder().decode(authHeader.substring(6).trim()));
            int colonIdx = decoded.indexOf(':');
            if (colonIdx > 0) {
                return new Credentials(decoded.substring(0, colonIdx), decoded.substring(colonIdx + 1));
            }
        } catch (Exception e) {
            log.warn("Failed to decode Basic Auth header", e);
        }
        return null;
    }

    private record Credentials(String email, String password) {
    }

    private String errorJson(String message) {
        try {
            return objectMapper.writeValueAsString(Map.of("error", message == null ? "" : message));
        } catch (JsonProcessingException e) {
            return "{\"error\":\"error serialization failed\"}";
        }
    }

    // ──────────── Input JSON Schemas ────────────

    private static final String INPUT_SCHEMA_PROTEIN_STRUCTURE_PREDICT = """
            {
              "type": "object",
              "properties": {
                "recycleNums": {
                  "type": "integer",
                  "description": "结构refinement迭代次数，1~8，默认4",
                  "minimum": 1,
                  "maximum": 8,
                  "default": 4
                },
                "seqList": {
                  "type": "array",
                  "description": "蛋白质序列列表，仅允许1条序列",
                  "items": {
                    "type": "object",
                    "properties": {
                      "header": {"type": "string", "description": "序列名称（可选，最多512字符）"},
                      "seq": {"type": "string", "description": "标准氨基酸单字母序列（必填，最多1000字符），如 MKTVRQERLKSIVR..."}
                    },
                    "required": ["seq"]
                  },
                  "minItems": 1,
                  "maxItems": 1
                }
              },
              "required": ["seqList"]
            }""";

    private static final String INPUT_SCHEMA_STRUCT_EMBEDDING_SEARCH_PRO = """
            {
              "type": "object",
              "properties": {
                "db": {
                  "type": "array",
                  "description": "要搜索的数据库列表（必填）。可选值：ncbi_refseq_archaea, ncbi_refseq_virus, ncbi_refseq_bacteria, ncbi_refseq_fungi, ncbi_refseq_plants, uniprotkb_swissprot, hidata_genome_cyanobacteria",
                  "items": {"type": "string"}
                },
                "count": {
                  "type": "integer",
                  "description": "最多返回的命中条数，500~10000000",
                  "minimum": 500,
                  "maximum": 10000000
                },
                "radius": {
                  "type": "number",
                  "description": "相似度半径阈值，0~1，越小结果越严格（相似度越高）",
                  "minimum": 0,
                  "maximum": 1
                },
                "doAligner": {
                  "type": "boolean",
                  "description": "是否对命中序列做序列对齐",
                  "default": true
                },
                "doLink": {
                  "type": "boolean",
                  "description": "是否在结果中附带外部链接",
                  "default": true
                },
                "seqList": {
                  "type": "array",
                  "description": "蛋白质序列列表，仅允许1条序列",
                  "items": {
                    "type": "object",
                    "properties": {
                      "header": {"type": "string", "description": "序列名称（可选，最多512字符）"},
                      "seq": {"type": "string", "description": "标准氨基酸单字母序列（必填，最多2000字符）"}
                    },
                    "required": ["seq"]
                  },
                  "minItems": 1,
                  "maxItems": 1
                }
              },
              "required": ["db", "seqList"]
            }""";
}
