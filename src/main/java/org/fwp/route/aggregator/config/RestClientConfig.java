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

@Configuration
@EnableConfigurationProperties(ProviderProperties.class)
public class RestClientConfig {

    public static final String PROVIDER_1 = "provider-1";
    public static final String PROVIDER_2 = "provider-2";

    @Bean
    public Executor providerFetchExecutor() {
        return Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("provider-fetch-", 0).factory()
        );
    }

    @Bean
    public RouteProvider provider1(ProviderProperties props) {
        return new HttpRouteProvider(PROVIDER_1, buildRestClient(props.oneBaseUrl(), props),
                buildRetry(PROVIDER_1, props));
    }

    @Bean
    public RouteProvider provider2(ProviderProperties props) {
        return new HttpRouteProvider(PROVIDER_2, buildRestClient(props.twoBaseUrl(), props),
                buildRetry(PROVIDER_2, props));
    }

    private Retry buildRetry(String name, ProviderProperties props) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(props.retry().maxAttempts())
                .waitDuration(props.retry().waitDuration())
                .retryExceptions(RestClientException.class)
                .build();
        return Retry.of(name, config);
    }

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