package org.fwp.route.aggregator.repository;

import org.fwp.route.aggregator.entity.RouteEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RouteRepository extends JpaRepository<RouteEntity, String> {
    Page<RouteEntity> findAllByOrderByDepartureTimeDesc(Pageable pageable);
}
