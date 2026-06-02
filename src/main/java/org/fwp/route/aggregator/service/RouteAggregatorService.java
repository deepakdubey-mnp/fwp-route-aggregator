package org.fwp.route.aggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.fwp.route.aggregator.entity.RouteEntity;
import org.fwp.route.aggregator.kafka.RoutePublisher;
import org.fwp.route.aggregator.model.Route;
import org.fwp.route.aggregator.provider.RouteProvider;
import org.fwp.route.aggregator.repository.RouteRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    private final RouteRepository routeRepository;
    private final List<RouteProvider> providers;
    private final Executor providerFetchExecutor;
    private final RoutePublisher routePublisher;

    private static final int PAGE_SIZE = 100;

    public RouteAggregatorService(
            RouteRepository routeRepository,
            List<RouteProvider> providers,
            @Qualifier("providerFetchExecutor") Executor providerFetchExecutor,
            RoutePublisher routePublisher) {
        this.routeRepository = routeRepository;
        this.providers = providers;
        this.providerFetchExecutor = providerFetchExecutor;
        this.routePublisher = routePublisher;
    }

    @Cacheable(value = "routes", key = "#page", unless = "#result.isEmpty()")
    public List<Route> getRoutes(int page) {
        List<Route> routes = routeRepository
                .findAllByOrderByDepartureTimeDesc(PageRequest.of(page, PAGE_SIZE))
                .map(RouteAggregatorService::toRoute)
                .toList();
        log.info("Read {} route(s) from Postgres [page={}, size={}]", routes.size(), page, PAGE_SIZE);
        return routes;
    }

    private static Route toRoute(RouteEntity e) {
        return new Route(e.getAirline(), e.getSourceAirport(), e.getDestinationAirport(),
                e.getCodeShare(), e.getStops(), e.getEquipment(), e.getDepartureTime());
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