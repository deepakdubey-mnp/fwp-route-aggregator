package org.fwp.route.aggregator.kafka;

import lombok.extern.slf4j.Slf4j;
import org.fwp.route.aggregator.model.Route;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Null-object adapter — active when kafka.enabled is false or not set.
 *
 * Mirrors RouteEventProducer's @ConditionalOnProperty exactly (inverse value,
 * matchIfMissing=true) so the two are mutually exclusive and Spring's bean
 * scanning order never leaves RoutePublisher unsatisfied.
 *
 * Eliminates the nullable field + null guard that was in RouteAggregatorService.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "kafka.enabled", havingValue = "false", matchIfMissing = true)
public class NoOpRoutePublisher implements RoutePublisher {

    @Override
    public void publish(List<Route> routes) {
        log.warn("No publisher configured — {} route(s) fetched but not forwarded. "
                + "Set kafka.enabled=true or configure a webhook to activate publishing.", routes.size());
    }

    @Override
    public String destination() {
        return "none";
    }
}