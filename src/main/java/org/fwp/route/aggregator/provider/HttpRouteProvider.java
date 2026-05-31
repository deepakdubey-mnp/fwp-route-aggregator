package org.fwp.route.aggregator.provider;

import io.github.resilience4j.retry.Retry;
import lombok.extern.slf4j.Slf4j;
import org.fwp.route.aggregator.model.Route;
import org.springframework.web.client.RestClient;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
public class HttpRouteProvider implements RouteProvider {

    private final String providerName;
    private final RestClient restClient;
    private final Retry retry;

    public HttpRouteProvider(String providerName, RestClient restClient, Retry retry) {
        this.providerName = providerName;
        this.restClient = restClient;
        this.retry = retry;
    }

    @Override
    public List<Route> fetchRoutes() {
        try {
            Route[] routes = Retry.decorateSupplier(retry,
                    () -> restClient.get().retrieve().body(Route[].class)
            ).get();
            List<Route> result = routes != null ? Arrays.asList(routes) : Collections.emptyList();
            log.debug("Provider [{}] returned {} routes", providerName, result.size());
            return result;
        } catch (Exception ex) {
            log.error("Provider [{}] unavailable after {} attempt(s): {}", providerName,
                    retry.getMetrics().getNumberOfFailedCallsWithRetryAttempt() + 1, ex.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    public String name() {
        return providerName;
    }
}