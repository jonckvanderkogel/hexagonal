package com.bullit.application.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;

@Component
public final class GarageAdminClient {

    private final RestClient client;
    private final ObjectMapper mapper;

    public GarageAdminClient(GarageAdminProperties props, ObjectMapper mapper) {
        this.client = RestClient.builder()
                .baseUrl(props.baseUrl())
                .defaultHeader("Authorization", "Bearer " + props.token())
                .build();
        this.mapper = mapper;
    }

    public S3Credentials createAccessKey(String name) {
        var response = client.post()
                .uri("/v2/CreateKey")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .body(Map.of("name", name))
                .retrieve()
                .body(String.class);

        if (response == null || response.isBlank()) {
            throw new IllegalStateException("Garage admin CreateKey returned empty response");
        }
        return parseCredentials(response);
    }

    public void ensureBucketAndPermissions(String bucket, String accessKeyId) {
        var bucketId = findBucketIdByGlobalAlias(bucket)
                .orElseGet(() -> createBucketWithGlobalAlias(bucket));

        allowKeyReadWrite(bucketId, accessKeyId);
    }

    private Optional<String> findBucketIdByGlobalAlias(String globalAlias) {
        var json = client.get()
                .uri("/v2/ListBuckets")
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .body(String.class);

        if (json == null || json.isBlank()) return Optional.empty();

        try {
            var root = mapper.readTree(json);
            return StreamSupport.stream(root.spliterator(), false)
                    .filter(n -> hasGlobalAlias(n, globalAlias))
                    .map(n -> n.get("id"))
                    .filter(Objects::nonNull)
                    .map(JsonNode::asText)
                    .filter(s -> !s.isBlank())
                    .findFirst();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse ListBuckets response", e);
        }
    }

    private boolean hasGlobalAlias(JsonNode bucketNode, String globalAlias) {
        var aliases = bucketNode.get("globalAliases");
        if (aliases == null || !aliases.isArray()) return false;

        return StreamSupport.stream(aliases.spliterator(), false)
                .filter(JsonNode::isTextual)
                .map(JsonNode::asText)
                .anyMatch(globalAlias::equals);
    }

    private String createBucketWithGlobalAlias(String globalAlias) {
        var json = client.post()
                .uri("/v2/CreateBucket")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .body(Map.of("globalAlias", globalAlias))
                .retrieve()
                .body(String.class);

        if (json == null || json.isBlank()) {
            throw new IllegalStateException("Garage admin CreateBucket returned empty response");
        }

        try {
            var root = mapper.readTree(json);
            var id = root.get("id");
            if (id == null || !id.isTextual() || id.asText().isBlank()) {
                throw new IllegalStateException("CreateBucket response did not contain bucket id");
            }
            return id.asText();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse CreateBucket response", e);
        }
    }

    private void allowKeyReadWrite(String bucketId, String accessKeyId) {
        var body = Map.of(
                "bucketId", bucketId,
                "accessKeyId", accessKeyId,
                "permissions", Map.of("read", true, "write", true, "owner", true)
        );

        client.post()
                .uri("/v2/AllowBucketKey")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .toBodilessEntity();
    }

    private S3Credentials parseCredentials(String json) {
        try {
            JsonNode root = mapper.readTree(json);

            var accessKey = firstText(root, "accessKeyId", "access_key_id", "id", "accessKey");
            var secretKey = firstText(root, "secretKey", "secret_key", "secretAccessKey", "secret");

            return S3Credentials.of(accessKey, secretKey);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse Garage CreateKey response", e);
        }
    }

    private static String firstText(JsonNode root, String... fields) {
        return Arrays
                .stream(fields)
                .map(root::get)
                .filter(n -> Objects.nonNull(n) && n.isTextual() && !n.asText().isBlank())
                .map(JsonNode::asText)
                .findFirst()
                .orElse(null);
    }
}