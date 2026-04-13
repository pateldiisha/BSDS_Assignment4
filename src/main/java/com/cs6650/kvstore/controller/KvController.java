package com.cs6650.kvstore.controller;

import com.cs6650.kvstore.model.GetResponse;
import com.cs6650.kvstore.model.KvEntry;
import com.cs6650.kvstore.model.PutRequest;
import com.cs6650.kvstore.model.PutResponse;
import com.cs6650.kvstore.service.KvStoreService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kv")
public class KvController {

    private final KvStoreService kvStoreService;

    public KvController(KvStoreService kvStoreService) {
        this.kvStoreService = kvStoreService;
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("ok");
    }

    @PostMapping
    public ResponseEntity<?> put(@RequestBody PutRequest request) {
        if (request == null || request.getKey() == null || request.getKey().trim().isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Key cannot be null or empty");
        }

        String value = request.getValue();
        if (value == null) {
            value = "";
        }

        try {
            KvEntry entry = kvStoreService.handleClientWrite(request.getKey(), value);
            PutResponse response = new PutResponse(entry.getKey(), entry.getVersion());
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.getMessage());
        } catch (RuntimeException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(e.getMessage());
        }
    }

    @GetMapping("/{key}")
    public ResponseEntity<?> get(@PathVariable String key) {
        KvEntry entry = kvStoreService.handleClientRead(key);

        if (entry == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("Key not found");
        }

        GetResponse response = new GetResponse(
                entry.getKey(),
                entry.getValue(),
                entry.getVersion()
        );

        return ResponseEntity.ok(response);
    }

    @GetMapping("/local/{key}")
    public ResponseEntity<?> localRead(@PathVariable String key) {
        KvEntry entry = kvStoreService.localRead(key);

        if (entry == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("Key not found");
        }

        GetResponse response = new GetResponse(
                entry.getKey(),
                entry.getValue(),
                entry.getVersion()
        );

        return ResponseEntity.ok(response);
    }

    @GetMapping("/internal/read/{key}")
    public ResponseEntity<?> internalRead(@PathVariable String key) {
        KvEntry entry = kvStoreService.localRead(key);

        if (entry == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("Key not found");
        }

        return ResponseEntity.ok(entry);
    }

    @PostMapping("/internal/replicate")
    public ResponseEntity<?> replicate(@RequestBody KvEntry entry) {
        if (entry == null || entry.getKey() == null || entry.getKey().trim().isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Invalid replication request");
        }

        String value = entry.getValue();
        if (value == null) {
            entry.setValue("");
        }

        kvStoreService.replicate(entry);
        return ResponseEntity.ok("replicated");
    }
}