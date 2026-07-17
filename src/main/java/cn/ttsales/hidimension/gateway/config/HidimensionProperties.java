package cn.ttsales.hidimension.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 聚合 hidimension gateway 全部可配置项：
 * - hidimension 服务 base-url
 * - 轮询间隔与超时
 * - 各工具的 LLM-friendly description（来自 application.yml，与代码解耦）
 */
@Data
@ConfigurationProperties(prefix = "hidimension")
public class HidimensionProperties {

    /**
     * hidimension 服务地址（含 context-path），如 http://localhost:8080/lab/api
     */
    private String baseUrl;

    /**
     * 轮询配置
     */
    private Polling polling = new Polling();

    /**
     * 工具配置（description 在 application.yml 里维护）
     */
    private Tools tools = new Tools();

    @Data
    public static class Polling {
        /** 轮询间隔，默认 3 秒 */
        private Duration interval = Duration.ofSeconds(3);
        /** 轮询最大等待时长，默认 300 秒 */
        private Duration timeout = Duration.ofSeconds(300);
    }

    @Data
    public static class Tools {
        private ToolDef proteinStructurePredict = new ToolDef();
        private ToolDef structEmbeddingSearchPro = new ToolDef();
    }

    @Data
    public static class ToolDef {
        /** LLM-friendly tool description */
        private String description = "";
    }
}
