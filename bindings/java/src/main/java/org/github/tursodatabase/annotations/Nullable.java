package org.github.tursodatabase.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark nullable types.
 *
 * <p>This annotation is used to indicate that a method, field, or parameter can be null. It helps
 * in identifying potential nullability issues and improving code quality.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
public @interface Nullable {}
