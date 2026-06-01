package org.fwp.route.aggregator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.fwp.route.aggregator.kafka.RouteEventProducer;
import org.fwp.route.aggregator.model.Route;
import org.fwp.route.aggregator.provider.RouteProvider;
import org.springframework.beans.factory.annotation.Autowired;
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

@Slf4j
@Service
public class RouteAggregatorService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final List<RouteProvider> providers;
    private final Executor providerFetchExecutor;
    private final String keyPrefix;

    // Set by setter injection — null when kafka.enabled=false
    private RouteEventProducer routeEventProducer;

    @Autowired(required = false)
    public void setRouteEventProducer(RouteEventProducer routeEventProducer) {
        this.routeEventProducer = routeEventProducer;
    }

    public RouteAggregatorService(
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper,
            List<RouteProvider> providers,
            @Qualifier("providerFetchExecutor") Executor providerFetchExecutor,
            @Value("${routes.redis-key:routes}") String keyPrefix) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.providers = providers;
        this.providerFetchExecutor = providerFetchExecutor;
        this.keyPrefix = keyPrefix;
    }

    /**
     * Scans all Redis keys matching "{keyPrefix}:*" and deserialises each JSON value to a Route.
     * Keys are written by the publisher service as  routes:<datetime>  for enrichment traceability.
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
                    if (value != null) {
                        rawValues.add(value);
                    }
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
     * Fetches routes from all providers in parallel, deduplicates, then publishes
     * each route to the Kafka routes topic. Returns the number of routes dispatched.
     * No-op if Kafka is not enabled (kafka.enabled=false).
     */
    public int publishToKafka() {
       if (routeEventProducer == null) {
            log.warn("Kafka producer not available (kafka.enabled=false) — nothing published");
            return 0;
        }

        log.info("Fetching routes from {} provider(s) for Kafka publish", providers.size());

        List<CompletableFuture<List<Route>>> futures = providers.stream()
                .map(provider -> CompletableFuture.supplyAsync(() -> {
                    log.debug("Fetching from [{}] on thread [{}]",
                            provider.name(), Thread.currentThread().getName());
                    return provider.fetchRoutes();
                }, providerFetchExecutor))
                .toList();

        Map<RouteKey, Route> deduplicated = new LinkedHashMap<>();
        futures.stream()
                .map(CompletableFuture::join)
                .flatMap(Collection::stream)
                .forEach(route -> deduplicated.putIfAbsent(RouteKey.of(route), route));

        List<Route> routes = List.copyOf(deduplicated.values());

        if (routes.isEmpty()) {
            log.warn("No routes returned from providers — nothing published");
            return 0;
        }

        routeEventProducer.publishRoutes(routes);
        log.info("Dispatched {} unique route(s) to Kafka topic [{}]", routes.size(), RouteEventProducer.TOPIC);
        return routes.size();
    }

    private record RouteKey(String airline, String sourceAirport, String destinationAirport, int stops) {
        static RouteKey of(Route r) {
            return new RouteKey(r.airline(), r.sourceAirport(), r.destinationAirport(), r.stops());
        }
    }
}
