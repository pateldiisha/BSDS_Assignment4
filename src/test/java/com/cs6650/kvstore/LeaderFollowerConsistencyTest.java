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

import static org.junit.jupiter.api.Assertions.*;

public class LeaderFollowerConsistencyTest {

    private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();
    private final RestTemplate restTemplate = new RestTemplate();

    private static final String LEADER = "http://localhost:28080";
    private static final String F1 = "http://localhost:28081";
    private static final String F2 = "http://localhost:28082";
    private static final String F3 = "http://localhost:28083";
    private static final String F4 = "http://localhost:28084";

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
    void test1_strongConsistency_w5_readLeaderAfterAck() {
        startLeaderFollowerCluster(5, 1);

        PutResponse putResponse = put(LEADER, "t1", "value1");
        assertEquals(1, putResponse.getVersion());

        GetResponse getResponse = get(LEADER, "t1");
        assertEquals("value1", getResponse.getValue());
        assertEquals(1, getResponse.getVersion());
    }

    @Test
    void test2_strongConsistency_w5_r1_readFollowerAfterAck() {
        startLeaderFollowerCluster(5, 1);

        PutResponse putResponse = put(LEADER, "t2", "value2");
        assertEquals(1, putResponse.getVersion());

        GetResponse followerRead = get(F2, "t2");
        assertEquals("value2", followerRead.getValue());
        assertEquals(1, followerRead.getVersion());
    }

    @Test
    void test3_exposeInconsistency_w1_r1() {
        startLeaderFollowerCluster(1, 1);

        PutResponse putResponse = put(LEADER, "t3", "value3");
        assertEquals(1, putResponse.getVersion());

        boolean sawMissingOrStale =
                isMissingOrStale(F1, "t3", 1) ||
                        isMissingOrStale(F2, "t3", 1) ||
                        isMissingOrStale(F3, "t3", 1) ||
                        isMissingOrStale(F4, "t3", 1);

        assertTrue(
                sawMissingOrStale,
                "Expected at least one follower to be stale or missing during W=1 inconsistency window"
        );
    }

    private void startLeaderFollowerCluster(int writeQuorum, int readQuorum) {
        contexts.add(startNode(
                28080,
                "node0",
                "leader-follower",
                "leader",
                List.of(F1, F2, F3, F4),
                writeQuorum,
                readQuorum
        ));

        contexts.add(startNode(
                28081,
                "node1",
                "leader-follower",
                "follower",
                List.of(),
                writeQuorum,
                readQuorum
        ));

        contexts.add(startNode(
                28082,
                "node2",
                "leader-follower",
                "follower",
                List.of(),
                writeQuorum,
                readQuorum
        ));

        contexts.add(startNode(
                28083,
                "node3",
                "leader-follower",
                "follower",
                List.of(),
                writeQuorum,
                readQuorum
        ));

        contexts.add(startNode(
                28084,
                "node4",
                "leader-follower",
                "follower",
                List.of(),
                writeQuorum,
                readQuorum
        ));

        waitForHealth(LEADER);
        waitForHealth(F1);
        waitForHealth(F2);
        waitForHealth(F3);
        waitForHealth(F4);
    }

    private ConfigurableApplicationContext startNode(
            int port,
            String nodeId,
            String mode,
            String role,
            List<String> peers,
            int writeQuorum,
            int readQuorum
    ) {
        List<String> args = new ArrayList<>();
        args.add("--server.port=" + port);
        args.add("--kvstore.node-id=" + nodeId);
        args.add("--kvstore.mode=" + mode);
        args.add("--kvstore.role=" + role);
        args.add("--kvstore.quorum.write=" + writeQuorum);
        args.add("--kvstore.quorum.read=" + readQuorum);
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

    private boolean isMissingOrStale(String baseUrl, String key, int expectedVersion) {
        try {
            String url = baseUrl + "/kv/local/" + key;

            ResponseEntity<GetResponse> response =
                    restTemplate.exchange(url, HttpMethod.GET, null, GetResponse.class);

            GetResponse body = response.getBody();
            return body == null || body.getVersion() < expectedVersion;
        } catch (HttpClientErrorException.NotFound e) {
            return true;
        }
    }
}