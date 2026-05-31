package org.fwp.route.aggregator.web;

import lombok.RequiredArgsConstructor;
import org.fwp.route.aggregator.model.Route;
import org.fwp.route.aggregator.service.RouteAggregatorService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/routes")
@RequiredArgsConstructor
public class RoutesController {

    private final RouteAggregatorService aggregatorService;

    @GetMapping
    public List<Route> getRoutes() {
        return aggregatorService.getRoutes();
    }

    @DeleteMapping("/cache")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void evictCache() {
        aggregatorService.evictRoutes();
    }
}