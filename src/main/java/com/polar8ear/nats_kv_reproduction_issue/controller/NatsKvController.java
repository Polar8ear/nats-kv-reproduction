package com.polar8ear.nats_kv_reproduction_issue.controller;

import com.polar8ear.nats_kv_reproduction_issue.service.NatsKvService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/kv")
@RequiredArgsConstructor
@Slf4j
public class NatsKvController {

    private final NatsKvService natsKvService;

    @PostMapping("/write")
    public ResponseEntity<String> writeToKv(
            @RequestParam String key,
            @RequestBody Map<String, Object> data) {
        try {
            natsKvService.writeToKv(key, data);
            return ResponseEntity.ok("Successfully written to KV with key: " + key);
        } catch (Exception e) {
            log.error("Error writing to KV", e);
            return ResponseEntity.internalServerError().body("Failed to write to KV: " + e.getMessage());
        }
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("NATS KV Service is running");
    }
}
