package org.fwp.route.aggregator.kafka;

import lombok.extern.slf4j.Slf4j;
import org.fwp.route.aggregator.model.Route;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Null-object adapter — active when no real RoutePublisher is wired
 * (i.e. kafka.enabled=false and no webhook configured).
 *
 * Eliminates the nullable RouteEventProducer field and the null guard
 * in RouteAggregatorService. The service always has a non-null publisher;
 * enabling/disabling Kafka is an infrastructure concern resolved at startup,
 * not a runtime conditional in business logic.
 */
@Slf4j
@Component
@ConditionalOnMissingBean(RoutePublisher.class)
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