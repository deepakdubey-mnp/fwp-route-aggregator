package org.fwp.route.aggregator.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record Route(
        String airline,
        String sourceAirport,
        String destinationAirport,
        String codeShare,
        int stops,
        String equipment
) implements Serializable {}