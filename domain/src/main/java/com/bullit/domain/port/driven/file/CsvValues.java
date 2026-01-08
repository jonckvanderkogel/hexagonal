package com.bullit.domain.port.driven.file;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

public final class CsvValues {
    private CsvValues() {
    }

    public static Optional<Integer> optionalInt(CsvRow row, String field) {
        return optionalMapped(row, field, Integer::parseInt);
    }

    public static int requiredInt(CsvRow row, String field) {
        return requiredMapped(row, field, Integer::parseInt);
    }

    public static Optional<Long> optionalLong(CsvRow row, String field) {
        return optionalMapped(row, field, Long::parseLong);
    }

    public static long requiredLong(CsvRow row, String field) {
        return requiredMapped(row, field, Long::parseLong);
    }

    public static Optional<BigDecimal> optionalDecimal(CsvRow row, String field) {
        return optionalMapped(row, field, BigDecimal::new);
    }

    public static BigDecimal requiredDecimal(CsvRow row, String field) {
        return requiredMapped(row, field, BigDecimal::new);
    }

    public static Optional<UUID> optionalUuid(CsvRow row, String field) {
        return optionalMapped(row, field, UUID::fromString);
    }

    public static UUID requiredUuid(CsvRow row, String field) {
        return requiredMapped(row, field, UUID::fromString);
    }

    /**
     * Optional ISO-8601 UTC instant.
     * <p>
     * Expected format: {@code 2024-08-13T09:00:00Z}
     */
    public static Optional<Instant> optionalInstant(CsvRow row, String field) {
        return optionalMapped(row, field, CsvValues::parseInstantUtc);
    }

    /**
     * Required ISO-8601 UTC instant.
     * <p>
     * Expected format: {@code 2024-08-13T09:00:00Z}
     */
    public static Instant requiredInstant(CsvRow row, String field) {
        return requiredMapped(row, field, CsvValues::parseInstantUtc);
    }

    private static Instant parseInstantUtc(String value) {
        try {
            // Instant.parse enforces ISO-8601 and UTC
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "Invalid UTC timestamp (expected ISO-8601, e.g. 2024-08-13T09:00:00Z): " + value, e
            );
        }
    }

    public static Optional<String> optionalText(CsvRow row, String field) {
        return row.findField(field)
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .filter(s -> !s.equalsIgnoreCase("null"));
    }

    public static String requiredText(CsvRow row, String field) {
        return optionalText(row, field)
                .orElseThrow(() ->
                        new IllegalArgumentException("Missing required CSV field: " + field));
    }

    private static <T> Optional<T> optionalMapped(
            CsvRow row,
            String field,
            Function<String, T> mapper
    ) {
        try {
            return optionalText(row, field).map(mapper);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid CSV value for field '%s'".formatted(field), e);
        }
    }

    private static <T> T requiredMapped(
            CsvRow row,
            String field,
            Function<String, T> mapper
    ) {
        try {
            return mapper.apply(requiredText(row, field));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid CSV value for field '%s'".formatted(field), e);
        }
    }
}