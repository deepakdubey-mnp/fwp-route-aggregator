package org.fwp.route.aggregator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "routes.provider")
public record ProviderProperties(
        String oneBaseUrl,
        String twoBaseUrl,
        Duration connectTimeout,
        Duration readTimeout,
        RetryProperties retry
) {
    public ProviderProperties {
        if (connectTimeout == null) connectTimeout = Duration.ofSeconds(3);
        if (readTimeout == null) readTimeout = Duration.ofSeconds(5);
        if (retry == null) retry = new RetryProperties(3, Duration.ofMillis(500));
    }

    public record RetryProperties(int maxAttempts, Duration waitDuration) {
        public RetryProperties {
            if (maxAttempts == 0) maxAttempts = 3;
            if (waitDuration == null) waitDuration = Duration.ofMillis(500);
        }
    }
}