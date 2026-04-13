package com.cs6650.kvstore.service;

import com.cs6650.kvstore.config.KvStoreProperties;
import com.cs6650.kvstore.model.KvEntry;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class KvStoreService {

    private final ConcurrentMap<String, KvEntry> store = new ConcurrentHashMap<>();
    private final KvStoreProperties properties;
    private final RestTemplate restTemplate;

    public KvStoreService(KvStoreProperties properties, RestTemplate restTemplate) {
        this.properties = properties;
        this.restTemplate = restTemplate;
    }

    public KvEntry handleClientWrite(String key, String value) {
        if (isLeaderlessMode()) {
            return putAsCoordinator(key, value);
        }

        if (!isLeader()) {
            throw new IllegalStateException("Writes are only allowed on leader in leader-follower mode");
        }

        return putAsCoordinator(key, value);
    }

    public KvEntry handleClientRead(String key) {
        int readQuorum = properties.getQuorum().getRead();

        if (readQuorum <= 1) {
            return get(key);
        }

        List<KvEntry> responses = new ArrayList<>();

        KvEntry local = get(key);
        if (local != null) {
            responses.add(local);
        }

        for (String peer : properties.getPeers()) {
            if (responses.size() >= readQuorum) {
                break;
            }

            KvEntry peerEntry = readFromPeer(peer, key);
            if (peerEntry != null) {
                responses.add(peerEntry);
            }
        }

        return responses.stream()
                .max(Comparator.comparingInt(KvEntry::getVersion))
                .orElse(null);
    }

    public synchronized KvEntry put(String key, String value) {
        simulateWriteDelay();

        KvEntry existing = store.get(key);
        int newVersion = (existing == null) ? 1 : existing.getVersion() + 1;

        KvEntry newEntry = new KvEntry(key, value, newVersion);
        store.put(key, newEntry);
        return newEntry;
    }

    public KvEntry putAsCoordinator(String key, String value) {
        KvEntry localEntry = put(key, value);
        int acknowledgements = 1; // local node counts
        int writeQuorum = properties.getQuorum().getWrite();

        List<String> peers = properties.getPeers();
        List<String> remainingPeers = new ArrayList<>();

        for (int i = 0; i < peers.size(); i++) {
            String peer = peers.get(i);

            // In leader-follower mode, once quorum is satisfied, return early
            // but continue replication to the remaining followers in background.
            if (!isLeaderlessMode() && acknowledgements >= writeQuorum) {
                remainingPeers.addAll(peers.subList(i, peers.size()));
                break;
            }

            boolean replicated = replicateToPeer(peer, localEntry);
            if (replicated) {
                acknowledgements++;
            }
        }

        if (acknowledgements < writeQuorum) {
            throw new RuntimeException("Write quorum not satisfied");
        }

        if (!isLeaderlessMode() && !remainingPeers.isEmpty()) {
            continueReplicationAsync(remainingPeers, localEntry);
        }

        return localEntry;
    }

    public KvEntry get(String key) {
        simulateReadDelay();
        return store.get(key);
    }

    public KvEntry localRead(String key) {
        simulateReadDelay();
        return store.get(key);
    }

    public void replicate(KvEntry entry) {
        simulateWriteDelay();

        KvEntry replicatedEntry = new KvEntry(
                entry.getKey(),
                entry.getValue(),
                entry.getVersion()
        );

        store.put(entry.getKey(), replicatedEntry);
    }

    public boolean isLeader() {
        return "leader".equalsIgnoreCase(properties.getRole());
    }

    public boolean isLeaderlessMode() {
        return "leaderless".equalsIgnoreCase(properties.getMode());
    }

    private void continueReplicationAsync(List<String> remainingPeers, KvEntry entry) {
        Thread backgroundReplication = new Thread(() -> {
            for (String peer : remainingPeers) {
                replicateToPeer(peer, entry);
            }
        });

        backgroundReplication.setDaemon(true);
        backgroundReplication.start();
    }

    private boolean replicateToPeer(String peerBaseUrl, KvEntry entry) {
        try {
            String url = peerBaseUrl + "/kv/internal/replicate";
            HttpEntity<KvEntry> request = new HttpEntity<>(entry);
            ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
            return response.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            return false;
        }
    }

    private KvEntry readFromPeer(String peerBaseUrl, String key) {
        try {
            String url = peerBaseUrl + "/kv/internal/read/" + key;
            ResponseEntity<KvEntry> response = restTemplate.getForEntity(url, KvEntry.class);
            return response.getBody();
        } catch (Exception e) {
            return null;
        }
    }

    private void simulateWriteDelay() {
        sleep(properties.getDelays().getWriteDelayMs());
    }

    private void simulateReadDelay() {
        sleep(properties.getDelays().getReadDelayMs());
    }

    private void sleep(long ms) {
        if (ms <= 0) {
            return;
        }

        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted during simulated delay", e);
        }
    }
}