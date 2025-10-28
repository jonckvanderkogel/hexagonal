package com.bullit.web.adapter.driving.http;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public abstract class AbstractHttpTest {
    private static final Pattern PLACEHOLDER = Pattern.compile("\\{([^}]+)}");

    protected final List<HttpMessageConverter<?>> converters =
            List.of(new MappingJackson2HttpMessageConverter(
                    JsonMapper.builder()
                            .addModule(new JavaTimeModule())
                            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                            .build()
            ));

    protected ServerRequest postJson(String path, String json) {
        var servlet = new MockHttpServletRequest("POST", path);
        servlet.setContentType(MediaType.APPLICATION_JSON_VALUE);
        servlet.setContent(json.getBytes(StandardCharsets.UTF_8));
        return ServerRequest.create(servlet, converters);
    }

    protected ServerRequest getWithPathVars(String pathTemplate, Map<String, String> vars) {
        var servlet = new MockHttpServletRequest("GET", pathTemplateWithValues(pathTemplate, vars));
        servlet.setAttribute(RouterFunctions.URI_TEMPLATE_VARIABLES_ATTRIBUTE, vars);
        return ServerRequest.create(servlet, converters);
    }

    private static String pathTemplateWithValues(String template, Map<String, String> vars) {
        return PLACEHOLDER
                .matcher(template)
                .replaceAll(mr -> Objects.toString(vars.get(mr.group(1)), mr.group(0)));
    }

    protected int status(ServerResponse response) {
        return response.statusCode().value();
    }

    protected String writeToString(ServerResponse response) throws Exception {
        var servletReq = new MockHttpServletRequest();
        var servletRes = new MockHttpServletResponse();
        ServerResponse.Context context = () -> converters;
        response.writeTo(servletReq, servletRes, context);
        return servletRes.getContentAsString();
    }
}