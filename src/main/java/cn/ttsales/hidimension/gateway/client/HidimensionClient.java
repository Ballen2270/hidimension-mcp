package cn.ttsales.hidimension.gateway.client;

import cn.ttsales.hidimension.gateway.config.HidimensionProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.timeout.ReadTimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.DigestUtils;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 纯 reactive 的 hidimension HTTP 客户端。
 * <p>
 * cookie 缓存：按 email 复用 hidimension 登录 cookie，命中且未过 TTL 直接用；
 * 若被 hidimension 判为失效(响应 code=401)则清缓存、重新登录、再试一次。
 */
@Slf4j
@Component
public class HidimensionClient {

    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    /** 终态 */
    private static final byte STATUS_COMPLETED = 4;
    private static final byte STATUS_FAILED = 36;
    private static final byte STATUS_CANCELLED = 16;
    private static final byte STATUS_REJECTED = -128;

    /** email -> 登录 cookie 缓存 */
    private final ConcurrentHashMap<String, CachedCookie> cookieCache = new ConcurrentHashMap<>();
    /** cookie 视为新鲜的时长，需 < hidimension 端 24h cookie TTL - 单次轮询 300s */
    private static final Duration COOKIE_FRESH_TTL = Duration.ofHours(20);
    /** 与 hidimension 前端保持一致：MD5(SALT + plaintextPassword) */
    private static final String PASSWORD_SALT = "497iF!98";

    public HidimensionClient(HidimensionProperties props, ObjectMapper objectMapper) {
        this(WebClient.builder().baseUrl(props.getBaseUrl()).build(), objectMapper);
    }

    HidimensionClient(WebClient webClient, ObjectMapper objectMapper) {
        this.webClient = webClient;
        this.objectMapper = objectMapper;
    }

    /** 创建任务的结果：带回所用 cookie 和任务类型路径，供后续轮询复用 */
    public record CreatedTask(String cookie, String taskPath, JsonNode info) {
    }

    private record CachedCookie(String cookie, Instant createdAt) {
    }

    // ──────────────────────────── 鉴权 / cookie 缓存 ────────────────────────────

    /** 取新鲜 cookie：缓存命中且未过 TTL 直接复用，否则登录 */
    private Mono<String> obtainCookie(String email, String password) {
        CachedCookie cached = cookieCache.get(email);
        if (cached != null && Duration.between(cached.createdAt(), Instant.now()).compareTo(COOKIE_FRESH_TTL) < 0) {
            return Mono.just(cached.cookie());
        }
        return loginFresh(email, password);
    }

    private Mono<String> loginFresh(String email, String password) {
        return login(email, password)
                .doOnNext(cookie -> cookieCache.put(email, new CachedCookie(cookie, Instant.now())));
    }

    private void invalidateCookie(String email) {
        cookieCache.remove(email);
    }

    /** 调 hidimension /auth/login 换 cookie。凭据错抛 HidimensionAuthException */
    private Mono<String> login(String email, String password) {
        String passwordMd5 = md5Password(password);
        return webClient.post()
                .uri(uriBuilder -> uriBuilder
                        .path("/auth/login")
                        .queryParam("email", email)
                        .queryParam("password", passwordMd5)
                        .build())
                .retrieve()
                .onStatus(status -> status.isError(), response ->
                        response.bodyToMono(String.class)
                                .flatMap(body -> Mono.error(new HidimensionAuthException("Login failed: " + body))))
                .toEntity(JsonNode.class)
                .flatMap(entity -> {
                    JsonNode body = entity.getBody();
                    if (body != null && body.has("code") && body.get("code").asInt() != 0) {
                        String msg = body.has("msg") ? body.get("msg").asText() : "unknown";
                        return Mono.error(new HidimensionAuthException("Login failed: " + msg));
                    }
                    return Optional.ofNullable(entity.getHeaders().getFirst("Set-Cookie"))
                            .map(setCookie -> Mono.just(setCookie.split(";")[0].trim()))
                            .orElseGet(() -> Mono.error(new HidimensionAuthException("No Set-Cookie in login response")));
                });
    }

    static String md5Password(String password) {
        return DigestUtils.md5DigestAsHex((PASSWORD_SALT + password).getBytes(StandardCharsets.UTF_8));
    }

    // ──────────────────────────── 创建任务(cookie 缓存 + 失效重试) ────────────────────────────

    public Mono<CreatedTask> createProteinStructurePredict(String email, String password, String requestBody) {
        return doCreate(email, password, requestBody, "/task/protein-structure-predict");
    }

    public Mono<CreatedTask> createStructEmbeddingSearchPro(String email, String password, String requestBody) {
        return doCreate(email, password, requestBody, "/task/struct-embedding-search/pro");
    }

    /**
     * 取 cookie 建任务；若 cookie 已失效(hidimension 返 code=401)则清缓存、重新登录、再试一次。
     */
    private Mono<CreatedTask> doCreate(String email, String password, String body, String path) {
        return obtainCookie(email, password)
                .flatMap(cookie -> postJson(path, cookie, body)
                        .map(info -> new CreatedTask(cookie, path, info)))
                .onErrorResume(HidimensionAuthException.class, e -> {
                    log.info("Cookie invalid for {}, re-login and retry create", email);
                    invalidateCookie(email);
                    return loginFresh(email, password)
                            .flatMap(cookie -> postJson(path, cookie, body)
                                    .map(info -> new CreatedTask(cookie, path, info)));
                });
    }

    // ──────────────────────────── 查询 / 取结果 / 轮询 ────────────────────────────

    public Mono<JsonNode> getTask(String cookie, String taskPath, String userTaskId) {
        return webClient.get()
                .uri(taskPath + "/{userTaskId}", userTaskId)
                .header("Cookie", cookie)
                .retrieve()
                .onStatus(status -> status.isError(), response ->
                        response.bodyToMono(String.class)
                                .flatMap(b -> Mono.error(new HidimensionApiException("getTask failed: " + b))))
                .bodyToMono(JsonNode.class)
                .switchIfEmpty(Mono.error(new HidimensionApiException("getTask returned an empty response")))
                .flatMap(this::extractData);
    }

    public Mono<JsonNode> getResult(String cookie, String taskId) {
        return webClient.get()
                .uri("/task-res/{taskId}", taskId)
                .header("Cookie", cookie)
                .retrieve()
                .onStatus(status -> status.isError(), response ->
                        response.bodyToMono(String.class)
                                .flatMap(b -> Mono.error(new HidimensionApiException("getResult failed: " + b))))
                .bodyToMono(JsonNode.class)
                .flatMap(this::extractData);
    }

    /**
     * 轮询任务直到终态，然后取结果。{@code Mono.expand} 非阻塞递归。
     * <p>注意 hidimension 两个 id 不同：UserTaskInfo.id 通过创建任务的同类型路径查状态；
     * UserTaskInfo.res 通过 /task-res/{taskId} 取结果。
     */
    public Mono<JsonNode> pollThenResult(String cookie, String taskPath, String userTaskId, String taskId,
                                          Duration pollInterval, Duration timeout) {
        return getTask(cookie, taskPath, userTaskId)
                .expand(info -> {
                    byte status = readStatus(info);
                    if (isTerminal(status)) {
                        return Mono.empty();
                    }
                    return Mono.delay(pollInterval).then(getTask(cookie, taskPath, userTaskId));
                })
                .last()
                .timeout(timeout, Mono.error(new HidimensionTimeoutException(
                        "Task " + taskId + " did not complete within " + timeout.getSeconds() + "s")))
                .onErrorMap(ReadTimeoutException.class,
                        e -> new HidimensionTimeoutException("Network timeout polling task " + taskId))
                .flatMap(info -> {
                    byte status = readStatus(info);
                    if (status == STATUS_COMPLETED) {
                        return getResult(cookie, taskId);
                    }
                    return Mono.error(toTerminalError(status, taskId));
                });
    }

    // ──────────────────────────── helper ────────────────────────────

    private Mono<JsonNode> postJson(String path, String cookie, String body) {
        return webClient.post()
                .uri(path)
                .header("Cookie", cookie)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(parseJson(body))
                .retrieve()
                .onStatus(status -> status.isError(), response ->
                        response.bodyToMono(String.class)
                                .flatMap(b -> Mono.error(new HidimensionApiException("POST " + path + " failed: " + b))))
                .bodyToMono(JsonNode.class)
                .flatMap(this::extractData);
    }

    /** hidimension 响应统一 {code,msg,data}；code!=0 报错，code==401 视为 cookie 失效(抛 AuthException) */
    private Mono<JsonNode> extractData(JsonNode response) {
        int code = response.has("code") ? response.get("code").asInt() : 0;
        if (code != 0) {
            String msg = response.has("msg") ? response.get("msg").asText() : "unknown error";
            if (code == 401) {
                return Mono.error(new HidimensionAuthException("Unauthorized (cookie may be expired): " + msg));
            }
            return Mono.error(new HidimensionApiException(msg));
        }
        return response.has("data") ? Mono.just(response.get("data")) : Mono.just(response);
    }

    private JsonNode parseJson(String body) {
        try {
            return objectMapper.readTree(body);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON body: " + body, e);
        }
    }

    private byte readStatus(JsonNode info) {
        if (info.has("status")) {
            return (byte) info.get("status").asInt();
        }
        return -1;
    }

    static boolean isTerminal(byte status) {
        return status == STATUS_COMPLETED || status == STATUS_FAILED
                || status == STATUS_CANCELLED || status == STATUS_REJECTED;
    }

    private RuntimeException toTerminalError(byte status, String taskId) {
        return switch (status) {
            case STATUS_FAILED -> new HidimensionTaskFailedException("Task " + taskId + " failed");
            case STATUS_CANCELLED -> new HidimensionTaskFailedException("Task " + taskId + " was cancelled");
            case STATUS_REJECTED -> new HidimensionTaskFailedException("Task " + taskId + " rejected: invalid params");
            default -> new HidimensionApiException("Task " + taskId + " ended with unknown status: " + status);
        };
    }

    // ──────────────────── 异常 ────────────────────

    public static class HidimensionAuthException extends RuntimeException {
        public HidimensionAuthException(String msg) {
            super(msg);
        }
    }

    public static class HidimensionApiException extends RuntimeException {
        public HidimensionApiException(String msg) {
            super(msg);
        }
    }

    public static class HidimensionTimeoutException extends RuntimeException {
        public HidimensionTimeoutException(String msg) {
            super(msg);
        }
    }

    public static class HidimensionTaskFailedException extends RuntimeException {
        public HidimensionTaskFailedException(String msg) {
            super(msg);
        }
    }
}
