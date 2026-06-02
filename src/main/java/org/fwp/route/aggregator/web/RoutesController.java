package org.fwp.route.aggregator.web;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.fwp.route.aggregator.kafka.RoutePublisher;
import org.fwp.route.aggregator.model.Route;
import org.fwp.route.aggregator.service.RouteAggregatorService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@Tag(name = "Routes", description = "Flight route aggregation — read, publish, and ingest via webhook")
@RestController
@RequestMapping("/routes")
@RequiredArgsConstructor
public class RoutesController {

    private final RouteAggregatorService aggregatorService;
    private final RoutePublisher routePublisher;

    @Operation(
            summary = "List aggregated routes",
            description = "Returns a page of deduplicated flight routes read from Postgres. " +
                    "Results are Redis-cached for 1 minute (configurable via `routes.cache.ttl`). " +
                    "Page size is fixed at 100 routes, ordered by departure time descending."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Page of routes (may be empty if no data for that page)",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            array = @ArraySchema(schema = @Schema(implementation = Route.class)))),
            @ApiResponse(responseCode = "500", description = "Unexpected server error", content = @Content)
    })
    @GetMapping
    public List<Route> getRoutes(
            @Parameter(description = "Zero-based page number (page size = 100)", example = "0")
            @RequestParam(defaultValue = "0") int page) {
        return aggregatorService.getRoutes(page);
    }

    @Operation(
            summary = "Trigger provider fetch and publish",
            description = "Pull flow: fetches routes concurrently from all configured external providers, " +
                    "deduplicates by `(airline, source, destination, stops)`, and forwards the unique set " +
                    "to the active RoutePublisher (Kafka in `local` and `prod` profiles). " +
                    "Returns the count of unique routes published and the destination identifier."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "202", description = "Routes fetched and dispatched",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(example = "{\"published\": 42, \"destination\": \"kafka:routes\"}"))),
            @ApiResponse(responseCode = "500", description = "Unexpected server error", content = @Content)
    })
    @PostMapping("/publish")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> publishRoutes() {
        int count = aggregatorService.publish();
        return Map.of(
                "published", count,
                "destination", routePublisher.destination()
        );
    }

    @Operation(
            summary = "Ingest routes via webhook",
            description = "Push flow: accepts a pre-fetched JSON array of routes from an external caller " +
                    "(e.g. a third-party provider webhook). The payload is deduplicated by the same key as " +
                    "the pull flow, then forwarded to the active RoutePublisher. " +
                    "Returns the count of unique routes published and the destination identifier."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "202", description = "Routes ingested and dispatched",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(example = "{\"published\": 15, \"destination\": \"kafka:routes\"}"))),
            @ApiResponse(responseCode = "400", description = "Malformed or empty request body", content = @Content),
            @ApiResponse(responseCode = "500", description = "Unexpected server error", content = @Content)
    })
    @io.swagger.v3.oas.annotations.parameters.RequestBody(
            description = "Array of route objects to ingest",
            required = true,
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    array = @ArraySchema(schema = @Schema(implementation = Route.class)),
                    examples = @ExampleObject(name = "sample", value = """
                            [
                              {
                                "airline": "AA",
                                "sourceAirport": "JFK",
                                "destinationAirport": "LHR",
                                "codeShare": null,
                                "stops": 0,
                                "equipment": "777",
                                "departureTime": "2025-11-01T08:30:00"
                              }
                            ]
                            """))
    )
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