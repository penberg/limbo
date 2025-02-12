package tech.turso.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Annotation to mark methods that use larger visibility for testing purposes. */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface VisibleForTesting {}
