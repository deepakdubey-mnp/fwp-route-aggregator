package org.fwp.route.aggregator.config;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.fwp.route.aggregator.provider.HttpRouteProvider;
import org.fwp.route.aggregator.provider.RouteProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * RestClientConfig — Spring configuration class that wires together all the
 * infrastructure beans needed to call external flight-data providers.
 *
 * <p><b>Why this class exists:</b>
 * Each external provider needs three collaborating objects:
 * <ol>
 *   <li>A {@link RestClient} configured with the provider's base URL and timeouts.</li>
 *   <li>A Resilience4j {@link Retry} instance that retries failed HTTP calls.</li>
 *   <li>An {@link HttpRouteProvider} that combines the above two into a single
 *       {@link RouteProvider} bean consumed by {@code RouteAggregatorService}.</li>
 * </ol>
 * This class creates all three per provider in one place, keeping construction logic
 * out of service classes and making it trivial to add a third provider — just add a
 * new {@code @Bean} method following the same pattern.
 *
 * <p>{@code @EnableConfigurationProperties} activates {@link ProviderProperties} so
 * Spring Boot binds the {@code routes.provider.*} YAML namespace into it and makes it
 * available for injection here.
 */
@Configuration
@EnableConfigurationProperties(ProviderProperties.class)
public class RestClientConfig {

    /** Logical name used as the Resilience4j retry identifier for Provider 1. */
    public static final String PROVIDER_1 = "provider-1";

    /** Logical name used as the Resilience4j retry identifier for Provider 2. */
    public static final String PROVIDER_2 = "provider-2";

    /**
     * Creates a virtual-thread-per-task {@link Executor} used by
     * {@code RouteAggregatorService} to fetch all providers concurrently.
     *
     * <p>Each provider call gets its own virtual thread (named {@code provider-fetch-N}).
     * Virtual threads are ideal here because provider HTTP calls are IO-bound — they park
     * cheaply while waiting for the network instead of blocking a platform (carrier) thread.
     * This allows hundreds of concurrent fetches with minimal memory overhead compared
     * to a fixed thread pool.
     */
    @Bean
    public Executor providerFetchExecutor() {
        return Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("provider-fetch-", 0).factory()
        );
    }

    /**
     * Registers Provider 1 as a {@link RouteProvider} Spring bean.
     *
     * <p>Uses {@code routes.provider.one-base-url} as the HTTP base URL.
     * The service auto-discovers this bean via {@code List<RouteProvider>} injection —
     * no explicit wiring in the service layer is needed.
     */
    @Bean
    public RouteProvider provider1(ProviderProperties props) {
        return new HttpRouteProvider(PROVIDER_1, buildRestClient(props.oneBaseUrl(), props),
                buildRetry(PROVIDER_1, props));
    }

    /**
     * Registers Provider 2 as a {@link RouteProvider} Spring bean.
     *
     * <p>Uses {@code routes.provider.two-base-url} as the HTTP base URL.
     * Identical construction pattern to {@code provider1} — adding a Provider 3
     * would simply mean adding another {@code @Bean} method here.
     */
    @Bean
    public RouteProvider provider2(ProviderProperties props) {
        return new HttpRouteProvider(PROVIDER_2, buildRestClient(props.twoBaseUrl(), props),
                buildRetry(PROVIDER_2, props));
    }

    /**
     * Builds a Resilience4j {@link Retry} instance for the given provider.
     *
     * <p>The retry is configured to:
     * <ul>
     *   <li>Retry up to {@code maxAttempts} times (initial attempt + retries).</li>
     *   <li>Wait {@code waitDuration} between each attempt (fixed back-off).</li>
     *   <li>Only trigger on {@link RestClientException} — network/HTTP errors —
     *       so non-retriable application errors are not retried needlessly.</li>
     * </ul>
     * After all attempts are exhausted, {@link HttpRouteProvider} falls back to an
     * empty list, allowing the aggregator to degrade gracefully using the other provider.
     *
     * <p>No AOP proxy or {@code RetryRegistry} is needed; the {@code Retry} instance
     * is passed directly to {@link HttpRouteProvider} at construction time.
     *
     * @param name  logical provider name, used as the Resilience4j metric label
     * @param props bound configuration containing retry settings
     * @return a configured {@link Retry} instance
     */
    private Retry buildRetry(String name, ProviderProperties props) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(props.retry().maxAttempts())
                .waitDuration(props.retry().waitDuration())
                .retryExceptions(RestClientException.class)
                .build();
        return Retry.of(name, config);
    }

    /**
     * Builds a {@link RestClient} for the given base URL with connect and read timeouts.
     *
     * <p>{@link SimpleClientHttpRequestFactory} is used (blocking, JDK
     * {@code HttpURLConnection} under the hood) rather than the reactive WebClient.
     * This is intentional — the service uses virtual threads for concurrency, so
     * blocking IO is acceptable and keeps the stack simple (no Reactor dependency).
     * Connect and read timeouts are sourced from {@link ProviderProperties} so they
     * can be tuned per environment without a code change.
     *
     * @param baseUrl the provider's root URL (e.g. the Lambda function URL)
     * @param props   bound configuration containing timeout values
     * @return a fully configured {@link RestClient} instance
     */
    private RestClient buildRestClient(String baseUrl, ProviderProperties props) {
        var factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(props.connectTimeout());
        factory.setReadTimeout(props.readTimeout());
        return RestClient.builder()
                .requestFactory(factory)
                .baseUrl(baseUrl)
                .build();
    }
}