package org.fwp.route.aggregator.web;

import lombok.RequiredArgsConstructor;
import org.fwp.route.aggregator.kafka.RoutePublisher;
import org.fwp.route.aggregator.model.Route;
import org.fwp.route.aggregator.service.RouteAggregatorService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/routes")
@RequiredArgsConstructor
public class RoutesController {

    private final RouteAggregatorService aggregatorService;
    private final RoutePublisher routePublisher;

    @GetMapping
    public List<Route> getRoutes() {
        return aggregatorService.getRoutes();
    }

    /**
     * Pull flow — triggers a fetch from all configured providers, deduplicates,
     * and forwards to the active RoutePublisher (Kafka, webhook-out, no-op).
     */
    @PostMapping("/publish")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> publishRoutes() {
        int count = aggregatorService.publish();
        return Map.of(
                "published", count,
                "destination", routePublisher.destination()
        );
    }

    /**
     * Push flow — accepts a JSON array of Route objects from an external caller
     * (e.g. a third-party webhook), deduplicates, and forwards to the active
     * RoutePublisher. No provider fetch is involved; the caller owns the data.
     *
     * Both flows share the same deduplication logic and the same RoutePublisher
     * adapter, so enabling/disabling Kafka affects both identically.
     */
    @PostMapping("/webhook")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> receiveWebhook(@RequestBody List<Route> routes) {
        int count = aggregatorService.publish(routes);
        return Map.of(
                "published", count,
                "destination", routePublisher.destination()
        );
    }
}