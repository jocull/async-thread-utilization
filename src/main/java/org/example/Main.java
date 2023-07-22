package org.example;

import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.example.executor.CooperativeThread;
import org.example.executor.CooperativeThreadInterruptedException;
import org.example.executor.CooperativeThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootApplication
public class Main implements CommandLineRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    static RestTemplate restTemplate;
    static final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    static ExecutorService executor;
    static final List<String> hosts = List.of("localhost:3000");
//    static final List<String> hosts = List.of(
//            "localhost:3000",
//            "localhost:3001",
//            "localhost:3002",
//            "localhost:3003",
//            "localhost:3004",
//            "localhost:3005");
    static final Iterator<String> hostIterator = Iterators.cycle(hosts);
    static List<String> objectPool;
    static Iterator<String> objectPoolIterator;

    public static void main(String[] args) {
        try {
            if (args.length > 0 && args[0].equals("cooperative")) {
                executor = new CooperativeThreadPoolExecutor(1000, Runtime.getRuntime().availableProcessors() * 2);
            } else {
                executor = Executors.newFixedThreadPool(1000);
            }
            LOGGER.info("Running with executor type: {}", executor.getClass().getName());

            LOGGER.info("STARTING THE APPLICATION");
            SpringApplication.run(Main.class, args);
            LOGGER.info("APPLICATION FINISHED");
        } finally {
            executor.shutdown();
            scheduledExecutor.shutdown();
        }
    }

    @Override
    public void run(String... args) throws Exception {
        final PoolingHttpClientConnectionManager poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager();
        poolingHttpClientConnectionManager.setDefaultMaxPerRoute(50);
        poolingHttpClientConnectionManager.setMaxTotal(400);

        final HttpClient httpClient = HttpClientBuilder.create()
                .setMaxConnPerRoute(50)
                .setMaxConnTotal(400)
                .setConnectionTimeToLive(1, TimeUnit.MINUTES)
                .setConnectionManager(poolingHttpClientConnectionManager)
                .build();

        final HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory(httpClient);
        requestFactory.setConnectTimeout(5_000);
        requestFactory.setReadTimeout(5_000);
        requestFactory.setConnectionRequestTimeout(0);

        restTemplate = new RestTemplate(requestFactory);
        restTemplate.setMessageConverters(List.of(
                new CooperativeGsonHttpMessageConverter()
        ));

        LOGGER.info("Filling the object pool...");
        objectPool = IntStream.range(0, 1000)
                .mapToObj(i -> executor.submit(() -> fastNetwork().data))
                .collect(Collectors.toList())
                .stream()
                .map(f -> {
                    try {
                        return f.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .map(data -> new Gson().toJson(data))
                .collect(Collectors.toList());
        objectPoolIterator = Iterators.cycle(objectPool);
        LOGGER.info("Done");

        final AtomicInteger progress = new AtomicInteger();
        final AtomicLong lastHeartbeat = new AtomicLong(System.currentTimeMillis());
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            final long now = System.currentTimeMillis();
            final long then = lastHeartbeat.getAndSet(now);
            LOGGER.info("Heartbeat: " + (now - then) + " @ " + progress.get() + " :: " + poolingHttpClientConnectionManager.getTotalStats());
        }, 0, 1, TimeUnit.SECONDS);

        final Instant totalStart = Instant.now();
        final List<OperationResult> result = IntStream.range(0, 10_000)
                .mapToObj(i -> {
                    final Instant outerStart = Instant.now();
                    return executor.submit(() -> {
                        final Instant innerStart = Instant.now();

                        final NetworkResult one = mockFastNetwork();
                        final NetworkResult two = mockFastNetwork();

                        final Map<String, Object> merged = new HashMap<>();
                        merged.putAll(one.data);
                        merged.putAll(two.data);
                        final NetworkResult mergedResult = parseNetworkResult(merged);
                        mockWriteNetwork(mergedResult);

                        final Duration outerDuration = Duration.between(outerStart, Instant.now());
                        final Duration innerDuration = Duration.between(innerStart, Instant.now());
                        final Duration waitDuration = Duration.between(outerStart, innerStart);

                        progress.incrementAndGet();
                        return new OperationResult(i, outerDuration, innerDuration, waitDuration);
                    });
                })
                .collect(Collectors.toList()).stream()
                .map(f -> {
                    try {
                        return f.get();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .collect(Collectors.toList());
        final Duration totalRuntime = Duration.between(totalStart, Instant.now());

        result.forEach(x -> LOGGER.info("Result: {}", x));
        LOGGER.info("Total runtime: {}", totalRuntime);
    }

    private static class OperationResult {
        final int index;
        final Duration outerDuration;
        final Duration innerDuration;
        final Duration waitDuration;

        public OperationResult(int index, Duration outerDuration, Duration innerDuration, Duration waitDuration) {
            this.index = index;
            this.outerDuration = outerDuration;
            this.innerDuration = innerDuration;
            this.waitDuration = waitDuration;
        }

        @Override
        public String toString() {
            return "OperationResult{" +
                    "index=" + index +
                    ", outerDuration=" + outerDuration +
                    ", innerDuration=" + innerDuration +
                    ", waitDuration=" + waitDuration +
                    '}';
        }
    }

    private static class NetworkResult {
        final Map<String, Object> data;
        final double parseResult;

        private NetworkResult(Map<String, Object> data, double parseResult) {
            this.data = Map.copyOf(data);
            this.parseResult = parseResult;
        }
    }

//    private double someBusyCpuWork() {
//        final long target = ThreadLocalRandom.current().nextLong(50_000_000L);
//        for (long i = 0; i < target; i++) {
//            // so busy
//        }
//        return ThreadLocalRandom.current().nextDouble();
//    }
//
//    private double manyFastNetwork() {
//        final long target = ThreadLocalRandom.current().nextLong(100);
//        double sum = 0;
//        for (long i = 0; i < target; i++) {
//            sum += fastNetwork();
//        }
//        return sum;
//    }

    private String getNextHost() {
        synchronized (hostIterator) {
            return hostIterator.next();
        }
    }

    @SuppressWarnings("unchecked")
    private NetworkResult fastNetwork() {
        return runNetwork("http://" + getNextHost() + "/");
    }

    @SuppressWarnings("unchecked")
    private NetworkResult slowNetwork() {
        return runNetwork("http://" + getNextHost() + "/slow");
    }

    @SuppressWarnings("unchecked")
    private NetworkResult mockFastNetwork() {
        final String data;
        synchronized (objectPoolIterator) {
            data = objectPoolIterator.next();
        }
        return parseNetworkResult(new Gson().fromJson(data, Map.class));
    }

    private NetworkResult mockSlowNetwork() {
        CooperativeThread.tryYieldFor(() -> {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextLong(10L, 50L));
            } catch (InterruptedException ex) {
                throw new CooperativeThreadInterruptedException(ex);
            }
        });
        return mockFastNetwork();
    }

    private void writeNetwork(NetworkResult networkResult) {
        CooperativeThread.tryYieldFor(() -> restTemplate.postForEntity("http://" + getNextHost() + "/", networkResult.data, Void.class));
    }

    private void mockWriteNetwork(NetworkResult networkResult) {
        final String data = new Gson().toJson(networkResult.data);
        if (data.length() == 0) {
            throw new IllegalArgumentException("Data not serialized as expected");
        }
    }

    @SuppressWarnings("unchecked")
    private NetworkResult runNetwork(String url) {
        final Map<String, Object> data = CooperativeThread.tryYieldFor(() -> restTemplate.getForEntity(url, Map.class).getBody());
        return parseNetworkResult(data);
    }

    private NetworkResult parseNetworkResult(Map<String, Object> data) {
        double sum = 0;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            sum += parseNetworkValue(entry.getValue());
        }
        return new NetworkResult(data, sum);
    }

    @SuppressWarnings("unchecked")
    private double parseNetworkValue(Object value) {
        if (value instanceof String) {
            return ((String) value).length();
        } else if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof Map) {
            return parseNetworkResult((Map<String, Object>) value).parseResult; // recurse
        } else if (value instanceof List) {
            double sum = 0;
            for (Object v : ((List<Object>) value)) {
                sum += parseNetworkValue(v); // recurse
            }
            return sum;
        } else {
            throw new IllegalArgumentException("Type " + value.getClass() + " is not understood?");
        }
    }
}