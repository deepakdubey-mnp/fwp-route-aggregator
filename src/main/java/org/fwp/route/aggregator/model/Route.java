package org.fwp.route.aggregator.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.io.Serializable;
import java.time.LocalDateTime;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "A deduplicated flight route from one of the aggregated providers")
public record Route(

        @Schema(description = "IATA airline code", example = "AA")
        String airline,

        @Schema(description = "IATA code of the departure airport", example = "JFK")
        String sourceAirport,

        @Schema(description = "IATA code of the arrival airport", example = "LHR")
        String destinationAirport,

        @Schema(description = "Code-share indicator ('Y' if the flight is operated by another carrier, absent otherwise)", example = "Y")
        String codeShare,

        @Schema(description = "Number of stops (0 = direct flight)", example = "0", minimum = "0")
        int stops,

        @Schema(description = "IATA aircraft equipment type code(s)", example = "738")
        String equipment,

        @Schema(description = "Scheduled departure date-time (ISO 8601, no timezone — local airport time)", example = "2025-11-01T08:30:00")
        LocalDateTime departureTime

) implements Serializable {}