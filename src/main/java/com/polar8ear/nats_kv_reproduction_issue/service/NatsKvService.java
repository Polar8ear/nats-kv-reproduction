package com.polar8ear.nats_kv_reproduction_issue.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.*;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.StorageType;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class NatsKvService {

    @Value("${nats.url:nats://localhost:4222}")
    private String natsUrl;

    @Value("${nats.bucket.name:test-bucket}")
    private String bucketName;

    @Value("${nats.bucket.ttl-seconds:1800}")
    private long ttlSeconds;

    private Connection natsConnection;
    private KeyValue keyValueStore;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ScheduledExecutorService readerExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService initializerExecutor = Executors.newSingleThreadScheduledExecutor();

    private static final List<String> FIXED_KEYS = List.of("key1", "key2", "key3");
    private volatile int lastKeyCount = -1;
    private volatile long lastKeyCountChangeTime = -1;

    @PostConstruct
    public void initialize() {
        try {
            log.info("Connecting to NATS at: {}", natsUrl);

            Options options = new Options.Builder()
                    .servers(natsUrl.split(","))
                    .connectionTimeout(Duration.ofSeconds(10))
                    .pingInterval(Duration.ofSeconds(10))
                    .reconnectWait(Duration.ofSeconds(2))
                    .maxReconnects(-1)
                    .build();

            natsConnection = Nats.connect(options);
            log.info("Connected to NATS successfully");

            // Create bucket if not present
            createOrGetBucket();

            // Start reader thread
            startReaderThread();

            // Start initializer thread
            startInitializerThread();

        } catch (Exception e) {
            log.error("Failed to initialize NATS connection", e);
            throw new RuntimeException("Failed to initialize NATS", e);
        }
    }

    private void createOrGetBucket() throws IOException, io.nats.client.JetStreamApiException {
        KeyValueManagement kvm = natsConnection.keyValueManagement();

        try {
            KeyValueConfiguration config = KeyValueConfiguration.builder()
                    .name(bucketName)
                    .storageType(StorageType.File)
                    .maxHistoryPerKey(3)
                    .replicas(3)
                    .compression(true)
                    .ttl(Duration.ofSeconds(ttlSeconds))
                    .maxBucketSize(10 * 1024 * 1024) // 10MB
                    .maxValueSize(8 * 1024) // 8KB
                    .build();

            kvm.create(config);
            log.info("Created KV bucket: {} with File storage, 3 replicas, 3 history, compression enabled, TTL {}s, max bucket 10MB, max value 8KB", bucketName, ttlSeconds);
        } catch (JetStreamApiException e) {
            throw new RuntimeException(e);
        }

        keyValueStore = natsConnection.keyValue(bucketName);
    }

    private void startReaderThread() {
        readerExecutor.scheduleAtFixedRate(() -> {
            try {
                // Create a fresh KeyValue instance for each read
                KeyValue kv = natsConnection.keyValue(bucketName);
                List<String> keys = kv.keys();
                int currentKeyCount = keys.size();

                if(lastKeyCount == -1) {
                    lastKeyCount = currentKeyCount;
                }

                // Check if key count has changed
                if (lastKeyCount != currentKeyCount) {
                    lastKeyCountChangeTime = System.currentTimeMillis();
                    log.warn("⚠️ KEY COUNT CHANGED! Previous: {}, Current: {}, Time: {}",
                            lastKeyCount, currentKeyCount, new java.util.Date(lastKeyCountChangeTime));
                    lastKeyCount = currentKeyCount;
                } else {
                    if (lastKeyCountChangeTime != -1) {
                        log.info("Reading KV bucket. Total keys: {}, Last change time: {}",
                                currentKeyCount, new java.util.Date(lastKeyCountChangeTime));
                    } else {
                        log.info("Reading KV bucket. Total keys: {}", currentKeyCount);
                    }
                }

                for (String key : keys) {
                    try {
                        KeyValueEntry entry = kv.get(key);
                        if (entry != null && entry.getValue() != null) {
                            String value = new String(entry.getValue());
                            log.debug("Key: {}, Value: {}, Revision: {}", key, value, entry.getRevision());
                        }
                    } catch (Exception e) {
                        log.error("Error reading key: {}", key, e);
                    }
                }
            } catch (Exception e) {
                log.error("Error in reader thread", e);
            }
        }, 0, 1, TimeUnit.SECONDS);

        log.info("Started KV reader thread with 5s interval");
    }

    private void startInitializerThread() {
        initializerExecutor.scheduleAtFixedRate(() -> {
            try {
                // Create a fresh KeyValue instance for each check
                KeyValue kv = natsConnection.keyValue(bucketName);

                for (String key : FIXED_KEYS) {
                    try {
                        KeyValueEntry entry = kv.get(key);

                        if (entry == null || entry.getValue() == null) {
                            // Key doesn't exist, initialize it
                            Map<String, Object> initialData = Map.of(
                                "initialized", true,
                                "timestamp", System.currentTimeMillis(),
                                "key", key
                            );
                            String jsonValue = objectMapper.writeValueAsString(initialData);
                            long revision = kv.put(key, jsonValue);
                            log.info("Initialized missing key: {} with revision: {}", key, revision);
                        } else {
                            log.debug("Key {} already exists with revision: {}", key, entry.getRevision());
                        }
                    } catch (Exception e) {
                        log.error("Error checking/initializing key: {}", key, e);
                    }
                }
            } catch (Exception e) {
                log.error("Error in initializer thread", e);
            }
        }, 0, 1, TimeUnit.SECONDS);

        log.info("Started KV initializer thread with 10s interval for keys: {}", FIXED_KEYS);
    }

    public void writeToKv(String key, Map<String, Object> data) {
        try {
            String jsonValue = objectMapper.writeValueAsString(data);
            long revision = keyValueStore.put(key, jsonValue);
            log.info("Written to KV - Key: {}, Revision: {}, Data: {}", key, revision, jsonValue);
        } catch (Exception e) {
            log.error("Failed to write to KV", e);
            throw new RuntimeException("Failed to write to KV", e);
        }
    }

    @PreDestroy
    public void cleanup() {
        try {
            log.info("Shutting down NATS KV Service");
            readerExecutor.shutdown();
            initializerExecutor.shutdown();
            if (!readerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                readerExecutor.shutdownNow();
            }
            if (!initializerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                initializerExecutor.shutdownNow();
            }
            if (natsConnection != null) {
                natsConnection.close();
            }
        } catch (Exception e) {
            log.error("Error during cleanup", e);
        }
    }
}
