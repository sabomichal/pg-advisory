package com.github.sabomichal.pgadvisory.spring;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Enables the {@code LockManager} without Spring Boot. Needs a {@link PgAdvisoryLockConnectionFactory} bean;
 * settings come from a {@code LockManagerSettings} bean (defaults otherwise).
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(PgAdvisoryConfiguration.class)
public @interface EnablePgAdvisory {
}
