package org.fwp.route.aggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.fwp.route.aggregator.model.Route;
import org.fwp.route.aggregator.provider.RouteProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Slf4j
@Service
public class RouteAggregatorService {

    private final List<RouteProvider> providers;
    private final Executor providerFetchExecutor;

    public RouteAggregatorService(List<RouteProvider> providers,
                                   @Qualifier("providerFetchExecutor") Executor providerFetchExecutor) {
        this.providers = providers;
        this.providerFetchExecutor = providerFetchExecutor;
    }

    @Cacheable(value = "routes", unless = "#result == null || #result.isEmpty()")
    public List<Route> getRoutes() {
        log.info("Fetching routes from {} provider(s)", providers.size());

        List<CompletableFuture<List<Route>>> futures = providers.stream()
                .map(provider -> CompletableFuture.supplyAsync(() -> {
                    log.debug("Fetching from [{}] on thread [{}]", provider.name(),
                            Thread.currentThread().getName());
                    return provider.fetchRoutes();
                }, providerFetchExecutor))
                .toList();

        Map<RouteKey, Route> deduplicated = new LinkedHashMap<>();

        futures.stream()
                .map(CompletableFuture::join)
                .flatMap(Collection::stream)
                .forEach(route -> deduplicated.putIfAbsent(RouteKey.of(route), route));

        log.info("Aggregated {} unique route(s)", deduplicated.size());
        return List.copyOf(deduplicated.values());
    }

    @CacheEvict(value = "routes", allEntries = true)
    public void evictRoutes() {
        log.info("Routes cache evicted");
    }

    private record RouteKey(String airline, String sourceAirport, String destinationAirport, int stops) {
        static RouteKey of(Route r) {
            return new RouteKey(r.airline(), r.sourceAirport(), r.destinationAirport(), r.stops());
        }
    }
}