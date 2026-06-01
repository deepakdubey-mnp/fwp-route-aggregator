package org.fwp.route.aggregator.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fwp.route.aggregator.model.Route;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "kafka.enabled", havingValue = "true")
public class RouteEventProducer {

    static final String TOPIC = "routes";

    private final KafkaTemplate<String, Route> kafkaTemplate;

    public void publishRoutes(List<Route> routes) {
        log.debug("Publishing {} route(s) to topic [{}]", routes.size(), TOPIC);
        routes.forEach(route ->
                kafkaTemplate.send(TOPIC, route.airline(), route)
                        .whenComplete((result, ex) -> {
                            if (ex != null) {
                                log.warn("Failed to publish route {}->{}: {}",
                                        route.sourceAirport(), route.destinationAirport(), ex.getMessage());
                            } else {
                                log.debug("Published route {}→{} offset={}",
                                        route.sourceAirport(), route.destinationAirport(),
                                        result.getRecordMetadata().offset());
                            }
                        })
        );
    }
}