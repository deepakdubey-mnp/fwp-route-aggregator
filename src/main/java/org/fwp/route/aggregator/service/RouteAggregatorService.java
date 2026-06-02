package org.fwp.route.aggregator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.fwp.route.aggregator.kafka.RoutePublisher;
import org.fwp.route.aggregator.model.Route;
import org.fwp.route.aggregator.provider.RouteProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Core service — reads aggregated routes from Redis and dispatches them via
 * a RoutePublisher adapter.
 *
 * This service knows nothing about Kafka, webhooks, or any other transport.
 * Swapping or adding a publish channel = add a new RoutePublisher bean;
 * zero changes needed here.
 */
@Slf4j
@Service
public class RouteAggregatorService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final List<RouteProvider> providers;
    private final Executor providerFetchExecutor;
    private final String keyPrefix;
    private final RoutePublisher routePublisher;

    public RouteAggregatorService(
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper,
            List<RouteProvider> providers,
            @Qualifier("providerFetchExecutor") Executor providerFetchExecutor,
            @Value("${routes.redis-key:routes}") String keyPrefix,
            RoutePublisher routePublisher) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.providers = providers;
        this.providerFetchExecutor = providerFetchExecutor;
        this.keyPrefix = keyPrefix;
        this.routePublisher = routePublisher;
    }

    /**
     * Reads all routes stored in Redis under keys matching "{keyPrefix}:*".
     * SCAN is used instead of KEYS to avoid blocking Redis on large keyspaces.
     */
    public List<Route> getRoutes() {
        String pattern = keyPrefix + ":*";
        List<String> rawValues = new ArrayList<>();

        redisTemplate.execute((RedisCallback<Void>) connection -> {
            ScanOptions options = ScanOptions.scanOptions()
                    .match(pattern)
                    .count(200)
                    .build();

            try (Cursor<byte[]> cursor = connection.keyCommands().scan(options)) {
                cursor.forEachRemaining(rawKey -> {
                    String key = new String(rawKey, StandardCharsets.UTF_8);
                    String value = redisTemplate.opsForValue().get(key);
                    if (value != null) rawValues.add(value);
                });
            }
            return null;
        });

        if (rawValues.isEmpty()) {
            log.warn("No routes found in Redis for pattern [{}]", pattern);
            return List.of();
        }

        List<Route> routes = rawValues.stream()
                .map(json -> {
                    try {
                        return objectMapper.readValue(json, Route.class);
                    } catch (Exception e) {
                        log.warn("Skipping malformed route entry: {}", e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();

        log.info("Read {} route(s) from Redis pattern [{}]", routes.size(), pattern);
        return routes;
    }

    /**
     * Pull flow: fetch from all providers concurrently, deduplicate, then delegate
     * to the active RoutePublisher adapter.
     *
     * Entry point: POST /routes/publish
     */
    public int publish() {
        log.info("Fetching routes from {} provider(s)", providers.size());

        List<CompletableFuture<List<Route>>> futures = providers.stream()
                .map(provider -> CompletableFuture.supplyAsync(() -> {
                    log.debug("Fetching from [{}] on thread [{}]",
                            provider.name(), Thread.currentThread().getName());
                    return provider.fetchRoutes();
                }, providerFetchExecutor))
                .toList();

        List<Route> fetched = futures.stream()
                .map(CompletableFuture::join)
                .flatMap(Collection::stream)
                .toList();

        return deduplicateAndPublish(fetched, "providers");
    }

    /**
     * Push flow: accept a pre-fetched list of routes (e.g. from a webhook caller),
     * deduplicate by the same key as the pull flow, then delegate to the active
     * RoutePublisher adapter.
     *
     * Entry point: POST /routes/webhook
     */
    public int publish(List<Route> incomingRoutes) {
        if (incomingRoutes == null || incomingRoutes.isEmpty()) {
            log.warn("Webhook received an empty payload — nothing published");
            return 0;
        }
        return deduplicateAndPublish(incomingRoutes, "webhook");
    }

    /**
     * Shared deduplication + publish step used by both pull and push flows.
     * Extracted to keep the two public entry points free of duplicated logic.
     */
    private int deduplicateAndPublish(List<Route> routes, String source) {
        Map<RouteKey, Route> deduplicated = new LinkedHashMap<>();
        routes.forEach(route -> deduplicated.putIfAbsent(RouteKey.of(route), route));

        List<Route> unique = List.copyOf(deduplicated.values());

        if (unique.isEmpty()) {
            log.warn("[{}] No routes after deduplication — nothing published", source);
            return 0;
        }

        routePublisher.publish(unique);
        log.info("[{}] Dispatched {} unique route(s) to [{}]", source, unique.size(), routePublisher.destination());
        return unique.size();
    }

    private record RouteKey(String airline, String sourceAirport, String destinationAirport, int stops) {
        static RouteKey of(Route r) {
            return new RouteKey(r.airline(), r.sourceAirport(), r.destinationAirport(), r.stops());
        }
    }
}