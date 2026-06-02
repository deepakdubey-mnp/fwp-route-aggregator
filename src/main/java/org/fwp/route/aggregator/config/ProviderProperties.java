package org.fwp.route.aggregator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * ProviderProperties — strongly-typed configuration record for all external
 * flight-data provider settings.
 *
 * <p><b>Why this class exists:</b>
 * Instead of scattering {@code @Value("${...}")} annotations across the codebase,
 * this record centralizes every tunable value related to provider HTTP calls into a
 * single, type-safe object. Spring Boot binds the {@code routes.provider.*} namespace
 * from {@code application.yaml} (or environment variables) directly into this record
 * at startup via {@link ConfigurationProperties}.
 *
 * <p><b>How it is used:</b>
 * {@code RestClientConfig} receives this record as a constructor-injected bean and
 * uses its values to build {@code RestClient} instances and Resilience4j {@code Retry}
 * configurations for each provider. No other class needs to touch raw property strings.
 *
 * <p><b>Environment-variable overrides (ECS / prod):</b>
 * <pre>
 *   routes.provider.one-base-url  → PROVIDER1_BASE_URL
 *   routes.provider.two-base-url  → PROVIDER2_BASE_URL
 * </pre>
 * Timeouts and retry settings are tuned at the JVM level via {@code application-prod.yaml}.
 *
 * <p><b>Record vs @ConfigurationProperties class:</b>
 * A Java {@code record} is used here because all fields are immutable after binding —
 * configuration should never change at runtime. The compact constructor provides
 * safe defaults without requiring nullable fields everywhere in the consuming code.
 */
@ConfigurationProperties(prefix = "routes.provider")
public record ProviderProperties(

        /* Base URL of Provider 1 (the first external flight-data lambda).
         * Maps to routes.provider.one-base-url / env var PROVIDER1_BASE_URL. */
        String oneBaseUrl,

        /* Base URL of Provider 2 (the second external flight-data lambda).
         * Maps to routes.provider.two-base-url / env var PROVIDER2_BASE_URL. */
        String twoBaseUrl,

        /* Maximum time to wait while establishing a TCP connection to a provider.
         * Defaults to 3 seconds if not set. Keeps the service from hanging
         * indefinitely when a provider endpoint is unreachable. */
        Duration connectTimeout,

        /* Maximum time to wait for the provider to stream back a complete HTTP response
         * after the connection is established. Defaults to 5 seconds.
         * Should be generous enough for slow lambdas on cold starts but short enough
         * to fail fast before the ALB idle timeout fires. */
        Duration readTimeout,

        /* Nested retry configuration applied to every provider HTTP call.
         * See RetryProperties for details. */
        RetryProperties retry

) {
    /**
     * Compact constructor — applies sensible defaults for any field that was not
     * supplied in the configuration file. This means YAML entries can be omitted
     * entirely and the service will still behave correctly in local development.
     *
     * <p>Defaults:
     * <ul>
     *   <li>{@code connectTimeout} → 3 s</li>
     *   <li>{@code readTimeout}    → 5 s</li>
     *   <li>{@code retry}          → 3 attempts, 500 ms back-off</li>
     * </ul>
     */
    public ProviderProperties {
        if (connectTimeout == null) connectTimeout = Duration.ofSeconds(3);
        if (readTimeout == null)    readTimeout    = Duration.ofSeconds(5);
        if (retry == null)          retry          = new RetryProperties(3, Duration.ofMillis(500));
    }

    /**
     * RetryProperties — nested record that controls Resilience4j retry behaviour
     * for each provider HTTP call.
     *
     * <p><b>Why it is nested:</b>
     * Retry settings are conceptually scoped to providers, so nesting them inside
     * {@code ProviderProperties} keeps the YAML namespace clean:
     * <pre>
     * routes:
     *   provider:
     *     retry:
     *       max-attempts: 3
     *       wait-duration: PT0.5S
     * </pre>
     *
     * <p><b>How it is used:</b>
     * {@code RestClientConfig.buildRetry()} reads these values to construct a
     * {@code io.github.resilience4j.retry.Retry} instance per provider. On a
     * {@code RestClientException} the provider is retried up to {@code maxAttempts}
     * times with a fixed {@code waitDuration} pause between attempts. After all
     * retries are exhausted, the provider falls back to an empty list so the
     * aggregator can still serve results from the other provider.
     */
    public record RetryProperties(

            /* Maximum number of call attempts (initial attempt + retries).
             * e.g. 3 means 1 initial call + 2 retries.
             * Defaults to 3 if omitted or set to 0. */
            int maxAttempts,

            /* Fixed pause between retry attempts.
             * Expressed as an ISO-8601 duration string in YAML, e.g. PT0.5S.
             * Defaults to 500 ms if omitted. */
            Duration waitDuration

    ) {
        /**
         * Compact constructor — guards against zero/null values that would produce
         * an invalid Resilience4j retry configuration.
         */
        public RetryProperties {
            if (maxAttempts == 0) maxAttempts = 3;
            if (waitDuration == null) waitDuration = Duration.ofMillis(500);
        }
    }
}