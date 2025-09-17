package com.bullit.domain.model;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;

import java.util.Set;
import java.util.stream.Collectors;

public final class DomainValidator {
    private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

    private DomainValidator() {}

    public static <T> T assertValid(T value) {
        var violations = VALIDATOR.validate(value);
        if (!violations.isEmpty()) throw new IllegalArgumentException(collectViolationMessages(violations));

        return value;
    }

    private static <T> String collectViolationMessages(Set<ConstraintViolation<T>> violations) {
        return violations
                .stream()
                .map(ConstraintViolation::getMessage)
                .collect(Collectors.joining("; "));
    }
}
