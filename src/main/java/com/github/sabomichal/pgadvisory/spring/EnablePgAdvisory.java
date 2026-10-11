package com.github.sabomichal.pgadvisory.spring;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Enables {@code PgAdvisory} without Spring Boot. Settings come from a {@code PgAdvisorySettings} bean
 * (defaults otherwise). Register a {@link FencingR2dbcTransactionManager} to fence transactions.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(PgAdvisoryConfiguration.class)
public @interface EnablePgAdvisory {
}
