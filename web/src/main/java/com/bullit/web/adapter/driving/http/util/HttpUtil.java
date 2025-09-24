package com.bullit.web.adapter.driving.http.util;

import com.bullit.domain.model.DomainValidator;
import jakarta.servlet.ServletException;
import org.springframework.web.servlet.function.ServerRequest;

import java.io.IOException;
import java.time.YearMonth;
import java.time.format.DateTimeParseException;
import java.util.UUID;

public class HttpUtil {
    private HttpUtil() {
    }

    public static <T> T parseAndValidateBody(ServerRequest req, Class<T> clazz) throws ServletException, IOException {
        var body = req.body(clazz);
        return DomainValidator.assertValid(body);
    }

    public static UUID parseUuid(String raw) {
        return UUID.fromString(raw);
    }

    public static YearMonth parseYearMonth(String raw) {
        try {
            return YearMonth.parse(raw); // expects yyyy-MM
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid period format, expected yyyy-MM");
        }
    }
}
