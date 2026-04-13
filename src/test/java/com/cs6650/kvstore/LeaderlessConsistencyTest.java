package com.cs6650.kvstore;

import com.cs6650.kvstore.model.GetResponse;
import com.cs6650.kvstore.model.PutResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class LeaderlessConsistencyTest {

    private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();
    private final RestTemplate restTemplate = new RestTemplate();

    private static final String N0 = "http://localhost:28180";
    private static final String N1 = "http://localhost:28181";
    private static final String N2 = "http://localhost:28182";
    private static final String N3 = "http://localhost:28183";
    private static final String N4 = "http://localhost:28184";

    @AfterEach
    void tearDown() {
        for (ConfigurableApplicationContext ctx : contexts) {
            try {
                ctx.close();
            } catch (Exception ignored) {
            }
        }
        contexts.clear();
    }

    @Test
    void testLeaderless_inconsistencyWindow_thenConsistencyAfterAck() throws Exception {
        startLeaderlessCluster();

        CompletableFuture<PutResponse> writeFuture =
                CompletableFuture.supplyAsync(() -> put(N2, "lk1", "leaderless-value"));

        Thread.sleep(50);

        boolean sawInconsistency =
                isMissing(N0, "lk1") ||
                        isMissing(N1, "lk1") ||
                        isMissing(N3, "lk1") ||
                        isMissing(N4, "lk1");

        PutResponse writeResponse = writeFuture.get();
        assertEquals(1, writeResponse.getVersion());

        GetResponse coordinatorRead = get(N2, "lk1");
        assertEquals("leaderless-value", coordinatorRead.getValue());
        assertEquals(1, coordinatorRead.getVersion());

        GetResponse peerRead = get(N4, "lk1");
        assertEquals("leaderless-value", peerRead.getValue());
        assertEquals(1, peerRead.getVersion());

        assertTrue(
                sawInconsistency,
                "Expected to observe at least one missing/stale read during leaderless propagation window"
        );
    }

    private void startLeaderlessCluster() {
        contexts.add(startNode(28180, "node0", List.of(N1, N2, N3, N4)));
        contexts.add(startNode(28181, "node1", List.of(N0, N2, N3, N4)));
        contexts.add(startNode(28182, "node2", List.of(N0, N1, N3, N4)));
        contexts.add(startNode(28183, "node3", List.of(N0, N1, N2, N4)));
        contexts.add(startNode(28184, "node4", List.of(N0, N1, N2, N3)));

        waitForHealth(N0);
        waitForHealth(N1);
        waitForHealth(N2);
        waitForHealth(N3);
        waitForHealth(N4);
    }

    private ConfigurableApplicationContext startNode(
            int port,
            String nodeId,
            List<String> peers
    ) {
        List<String> args = new ArrayList<>();
        args.add("--server.port=" + port);
        args.add("--kvstore.node-id=" + nodeId);
        args.add("--kvstore.mode=leaderless");
        args.add("--kvstore.role=node");
        args.add("--kvstore.quorum.write=5");
        args.add("--kvstore.quorum.read=1");
        args.add("--kvstore.delays.write-delay-ms=200");
        args.add("--kvstore.delays.read-delay-ms=50");

        for (int i = 0; i < peers.size(); i++) {
            args.add("--kvstore.peers[" + i + "]=" + peers.get(i));
        }

        return new SpringApplicationBuilder(KvServiceApplication.class)
                .profiles("test")
                .run(args.toArray(new String[0]));
    }

    private void waitForHealth(String baseUrl) {
        String url = baseUrl + "/kv/health";

        for (int i = 0; i < 40; i++) {
            try {
                ResponseEntity<String> response =
                        restTemplate.exchange(url, HttpMethod.GET, null, String.class);

                if (response.getStatusCode() == HttpStatus.OK) {
                    return;
                }
            } catch (ResourceAccessException ignored) {
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for health check", e);
            }
        }

        fail("Timed out waiting for node health: " + baseUrl);
    }

    private PutResponse put(String baseUrl, String key, String value) {
        String url = baseUrl + "/kv";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        String body = String.format("{\"key\":\"%s\",\"value\":\"%s\"}", key, value);
        HttpEntity<String> request = new HttpEntity<>(body, headers);

        ResponseEntity<PutResponse> response =
                restTemplate.exchange(url, HttpMethod.POST, request, PutResponse.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());
        return response.getBody();
    }

    private GetResponse get(String baseUrl, String key) {
        String url = baseUrl + "/kv/" + key;

        ResponseEntity<GetResponse> response =
                restTemplate.exchange(url, HttpMethod.GET, null, GetResponse.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        return response.getBody();
    }

    private boolean isMissing(String baseUrl, String key) {
        try {
            String url = baseUrl + "/kv/" + key;
            restTemplate.exchange(url, HttpMethod.GET, null, GetResponse.class);
            return false;
        } catch (HttpClientErrorException.NotFound e) {
            return true;
        }
    }
}