package com.github.sabomichal.pgadvisory.r2dbc;

import io.r2dbc.spi.R2dbcException;

final class SqlStates {

    static final String LOCK_NOT_AVAILABLE = "55P03";
    static final String SERIALIZATION_FAILURE = "40001";

    private SqlStates() {
    }

    /** Whether {@code error} or one of its causes is a database error with {@code sqlState}. */
    static boolean is(Throwable error, String sqlState) {
        for (var e = error; e != null; e = e.getCause()) {
            if (e instanceof R2dbcException r2dbc && sqlState.equals(r2dbc.getSqlState())) {
                return true;
            }
        }
        return false;
    }
}
