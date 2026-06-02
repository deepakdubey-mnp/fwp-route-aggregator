package org.fwp.route.aggregator.kafka;

import org.fwp.route.aggregator.model.Route;

import java.util.List;

/**
 * Port (output side) for route publishing.
 *
 * Adapters: KafkaRoutePublisher (kafka.enabled=true), NoOpRoutePublisher (fallback),
 * and any future WebhookRoutePublisher or SnsRoutePublisher — all without touching
 * RouteAggregatorService.
 */
public interface RoutePublisher {

    /**
     * Publish the deduplicated set of routes to the underlying transport.
     * Implementations must be non-blocking from the caller's perspective;
     * failures should be logged per-route rather than propagated.
     */
    void publish(List<Route> routes);

    /**
     * Human-readable description of the publish destination, e.g.
     * {@code "kafka:routes"} or {@code "webhook:https://..."}.
     * Surfaced in the POST /routes/publish response so callers know where
     * routes landed without coupling to a specific implementation.
     */
    String destination();
}