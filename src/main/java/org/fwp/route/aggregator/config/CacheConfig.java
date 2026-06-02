package org.fwp.route.aggregator.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * CacheConfig — Spring configuration class for cache-layer infrastructure beans.
 *
 * <p><b>Why this class exists:</b>
 * The route aggregator caches deduplicated flight-route data in Redis so that repeated
 * {@code GET /routes} calls are served from memory rather than re-fetching from all
 * external providers. This class is the designated home for any beans that support
 * that caching behavior.
 *
 * <p><b>Current responsibilities:</b>
 * <ul>
 *   <li>Registers a lenient {@link ObjectMapper} as a fallback Spring bean used by
 *       Jackson throughout the application — both for deserializing provider HTTP
 *       responses (via {@code RestClient}) and for JSON serialization in Spring MVC.</li>
 * </ul>
 *
 * <p><b>Why not an explicit RedisCacheManager bean?</b>
 * Spring Boot's Redis cache autoconfiguration reads TTL and serialization settings
 * directly from {@code application.yaml} ({@code spring.cache.redis.*} and
 * {@code routes.cache.ttl}). A custom {@code RedisCacheManager} bean would only be
 * needed if we required per-cache TTLs, a different key serializer, or custom eviction
 * policies. That complexity can be added here when required.
 */
@Configuration
public class CacheConfig {

    /**
     * Provides a default, application-wide Jackson {@link ObjectMapper}.
     *
     * <p>{@code @ConditionalOnMissingBean} ensures this bean is only registered when
     * no other {@code ObjectMapper} is already present in the application context
     * (e.g. one supplied by Spring MVC autoconfiguration or a test configuration).
     * This avoids silently overriding a customized mapper defined elsewhere.
     *
     * <p>{@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES} is set to
     * {@code false} so that the service remains resilient to provider schema
     * evolution: if an external flight-data API adds new JSON fields that are not
     * mapped in the {@code Route} record, Jackson will ignore them instead of
     * throwing a deserialization error.
     *
     * @return a lenient {@link ObjectMapper} instance
     */
    @Bean
    @ConditionalOnMissingBean(ObjectMapper.class)
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
}