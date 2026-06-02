package org.fwp.route.aggregator.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.fwp.route.aggregator.model.Route;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "kafka.enabled", havingValue = "true")
public class RouteEventProducer implements RoutePublisher {

    public static final String TOPIC = "routes";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public void publish(List<Route> routes) {
        log.debug("Publishing {} route(s) to topic [{}]", routes.size(), TOPIC);
        routes.forEach(route -> {
            String payload;
            try {
                payload = objectMapper.writeValueAsString(route);
            } catch (JsonProcessingException ex) {
                log.warn("Skipping route {}->{} because JSON serialization failed: {}",
                        route.sourceAirport(), route.destinationAirport(), ex.getMessage());
                return;
            }

            String key = routeKey(route);
            kafkaTemplate.send(TOPIC, key, payload)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.warn("Failed to publish route {}->{} key={}: {}",
                                    route.sourceAirport(), route.destinationAirport(), key, ex.getMessage());
                        } else {
                            log.debug("Published route {}→{} key={} offset={}",
                                    route.sourceAirport(), route.destinationAirport(),
                                    key, result.getRecordMetadata().offset());
                        }
                    });
        });
    }

    @Override
    public String destination() {
        return "kafka:" + TOPIC;
    }

    /**
     * Derives a stable, partition-consistent key from the route's identity fields.
     * Uses String.hashCode() which is guaranteed deterministic by the Java spec
     * (polynomial: s[0]*31^(n-1) + ... + s[n-1]).
     * Format: 8-char unsigned hex, e.g. "3a2f8b1c"
     */
    static String routeKey(Route route) {
        String composite = route.airline() + "|" +
                           route.sourceAirport() + "|" +
                           route.destinationAirport() + "|" +
                           route.stops();
        return "routes:"+String.format("%08x", composite.hashCode() & 0xFFFFFFFFL);
    }
}

@Configuration
@ConditionalOnProperty(name = "kafka.enabled", havingValue = "true")
class RouteKafkaTemplateConfig {

    @Bean
    ProducerFactory<String, String> routeProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.properties.security.protocol:}") String securityProtocol,
            @Value("${spring.kafka.properties.sasl.mechanism:}") String saslMechanism,
            @Value("${spring.kafka.properties.sasl.jaas.config:}") String saslJaasConfig,
            @Value("${spring.kafka.properties.sasl.client.callback.handler.class:}") String saslCallbackHandlerClass
    ) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        if (!securityProtocol.isBlank()) {
            config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        }
        if (!saslMechanism.isBlank()) {
            config.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        }
        if (!saslJaasConfig.isBlank()) {
            config.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        }
        if (!saslCallbackHandlerClass.isBlank()) {
            config.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, saslCallbackHandlerClass);
        }

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    KafkaTemplate<String, String> routeKafkaTemplate(ProducerFactory<String, String> routeProducerFactory) {
        return new KafkaTemplate<>(routeProducerFactory);
    }
}