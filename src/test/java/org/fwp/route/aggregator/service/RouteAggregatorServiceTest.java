package org.fwp.route.aggregator.service;

import org.fwp.route.aggregator.kafka.NoOpRoutePublisher;
import org.fwp.route.aggregator.kafka.RoutePublisher;
import org.fwp.route.aggregator.model.Route;
import org.fwp.route.aggregator.provider.RouteProvider;
import org.fwp.route.aggregator.repository.RouteRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RouteAggregatorServiceTest {

    @Mock RouteProvider provider1;
    @Mock RouteProvider provider2;
    @Mock RoutePublisher routePublisher;
    @Mock RouteRepository routeRepository;

    private RouteAggregatorService service;

    private static final Route BOM_DEL  = new Route("AI", "BOM", "DEL", null, 0, "320", LocalDateTime.now());
    private static final Route DEL_BOM  = new Route("6E", "DEL", "BOM", null, 0, "737",LocalDateTime.now());
    // Same composite key as BOM_DEL (airline+source+dest+stops match); only codeShare differs
    private static final Route DUPLICATE = new Route("AI", "BOM", "DEL", "Y", 0, "321", LocalDateTime.now());

    @BeforeEach
    void setUp() {
        // lenient: name() is called only inside the virtual-thread log.debug — not in every test
        lenient().when(provider1.name()).thenReturn("provider-1");
        lenient().when(provider2.name()).thenReturn("provider-2");

        service = new RouteAggregatorService(
                routeRepository,
                List.of(provider1, provider2),
                Executors.newVirtualThreadPerTaskExecutor(),
                routePublisher
        );
    }

    @Test
    void publish_delegatesToPublisher_andReturnsUniqueCount() {
        when(provider1.fetchRoutes()).thenReturn(List.of(BOM_DEL));
        when(provider2.fetchRoutes()).thenReturn(List.of(DEL_BOM));

        int count = service.publish();

        assertThat(count).isEqualTo(2);
        verify(routePublisher).publish(anyList());
    }

    @Test
    void publish_deduplicatesByKey_firstSeenWins() {
        when(provider1.fetchRoutes()).thenReturn(List.of(BOM_DEL));
        when(provider2.fetchRoutes()).thenReturn(List.of(DUPLICATE));

        int count = service.publish();

        assertThat(count).isEqualTo(1);
        verify(routePublisher).publish(List.of(BOM_DEL));
    }

    @Test
    void publish_whenAllProvidersReturnEmpty_doesNotCallPublisher() {
        when(provider1.fetchRoutes()).thenReturn(List.of());
        when(provider2.fetchRoutes()).thenReturn(List.of());

        int count = service.publish();

        assertThat(count).isZero();
        verify(routePublisher, never()).publish(anyList());
    }

    @Test
    void publish_whenOneProviderReturnsEmpty_stillPublishesOtherResults() {
        // HttpRouteProvider catches exceptions and returns [] — simulate that contract here
        when(provider1.fetchRoutes()).thenReturn(List.of(BOM_DEL));
        when(provider2.fetchRoutes()).thenReturn(List.of());

        int count = service.publish();

        assertThat(count).isEqualTo(1);
        verify(routePublisher).publish(List.of(BOM_DEL));
    }

    // --- Push (webhook) flow ---

    @Test
    void publishList_delegatesToPublisher_andReturnsUniqueCount() {
        int count = service.publish(List.of(BOM_DEL, DEL_BOM));

        assertThat(count).isEqualTo(2);
        verify(routePublisher).publish(List.of(BOM_DEL, DEL_BOM));
    }

    @Test
    void publishList_deduplicatesByKey_firstSeenWins() {
        int count = service.publish(List.of(BOM_DEL, DUPLICATE));

        assertThat(count).isEqualTo(1);
        verify(routePublisher).publish(List.of(BOM_DEL));
    }

    @Test
    void publishList_whenPayloadIsEmpty_doesNotCallPublisher() {
        int count = service.publish(List.of());

        assertThat(count).isZero();
        verify(routePublisher, never()).publish(anyList());
    }

    @Test
    void publishList_whenPayloadIsNull_doesNotCallPublisher() {
        int count = service.publish((List<Route>) null);

        assertThat(count).isZero();
        verify(routePublisher, never()).publish(anyList());
    }

    // --- NoOpRoutePublisher — isolated from service mocks to avoid strict-stub conflicts ---

    @Nested
    class NoOpPublisherTests {

        @Test
        void doesNotThrow_andDestinationIsNone() {
            NoOpRoutePublisher noOp = new NoOpRoutePublisher();
            noOp.publish(List.of(BOM_DEL, DEL_BOM)); // just logs — no exception
            assertThat(noOp.destination()).isEqualTo("none");
        }

        @Test
        void asServiceDependency_publish_returnsCountWithoutForwarding() {
            RouteProvider p1 = mock(RouteProvider.class);
            RouteProvider p2 = mock(RouteProvider.class);
            when(p1.fetchRoutes()).thenReturn(List.of(BOM_DEL));
            when(p2.fetchRoutes()).thenReturn(List.of(DEL_BOM));
            lenient().when(p1.name()).thenReturn("p1");
            lenient().when(p2.name()).thenReturn("p2");

            RouteAggregatorService noOpService = new RouteAggregatorService(
                    mock(RouteRepository.class),
                    List.of(p1, p2),
                    Executors.newVirtualThreadPerTaskExecutor(),
                    new NoOpRoutePublisher()
            );

            int count = noOpService.publish();

            // Routes are fetched and deduplicated even when the publisher is a no-op
            assertThat(count).isEqualTo(2);
        }
    }
}