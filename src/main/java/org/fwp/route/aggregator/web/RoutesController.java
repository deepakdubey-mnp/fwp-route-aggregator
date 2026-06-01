package org.fwp.route.aggregator.web;

import lombok.RequiredArgsConstructor;
import org.fwp.route.aggregator.kafka.RouteEventProducer;
import org.fwp.route.aggregator.model.Route;
import org.fwp.route.aggregator.service.RouteAggregatorService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
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

    @GetMapping
    public List<Route> getRoutes() {
        return aggregatorService.getRoutes();
    }

    @PostMapping("/publish")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> publishRoutes() {
        int count = aggregatorService.publishToKafka();
        return Map.of(
                "published", count,
                "topic", RouteEventProducer.TOPIC
        );
    }
}