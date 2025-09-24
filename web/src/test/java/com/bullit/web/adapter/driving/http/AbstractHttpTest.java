package com.bullit.web.adapter.driving.http;

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

public abstract class AbstractHttpTest {
    protected final List<HttpMessageConverter<?>> converters =
            List.of(new MappingJackson2HttpMessageConverter());

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
        var p = template;
        for (var e : vars.entrySet()) {
            p = p.replace("{" + e.getKey() + "}", e.getValue());
        }
        return p;
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