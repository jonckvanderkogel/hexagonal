package com.bullit.web.util;

import com.bullit.domain.model.DomainValidator;
import jakarta.servlet.ServletException;
import jakarta.validation.Validator;
import org.springframework.web.servlet.function.ServerRequest;

import java.io.IOException;
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
}
