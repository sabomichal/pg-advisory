package com.github.sabomichal.pgadvisory.spring.boot;

import io.r2dbc.spi.ConnectionFactory;

/**
 * Unpooled connection factory for the lock connection. A holder type, so that it does not compete with the
 * application {@link ConnectionFactory} for injection by type.
 */
public record PgAdvisoryLockConnectionFactory(ConnectionFactory connectionFactory) {
}
