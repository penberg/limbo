package org.github.tursodatabase.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation to skip nullable checks.
 *
 * <p>This annotation is used to mark methods, fields, or parameters that should be excluded from
 * nullable checks. It is typically applied to code that is still under development or requires
 * special handling.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
public @interface SkipNullableCheck {}
