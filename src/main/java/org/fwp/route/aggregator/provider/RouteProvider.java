package org.fwp.route.aggregator.provider;

import org.fwp.route.aggregator.model.Route;

import java.util.List;

public interface RouteProvider {
    List<Route> fetchRoutes();
    String name();
}