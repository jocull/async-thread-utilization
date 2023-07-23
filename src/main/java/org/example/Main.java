package org.example;

import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
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
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootApplication
public class Main implements CommandLineRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    static RestTemplate restTemplate;
    static final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    static ExecutorService executor;
    static int threadCount = 1000;
    static final List<String> hosts = List.of("localhost:3000");
    static final Iterator<String> hostIterator = Iterators.cycle(hosts);
    static List<String> objectPool;
    static Iterator<String> objectPoolIterator;

    public static void main(String[] args) {
        try {
            if (args.length > 1) {
                try {
                    threadCount = Integer.parseInt(args[1]);
                } catch (Exception ex) {
                    LOGGER.error("Can't parse thread count", ex);
                }
            }
            if (args.length > 0) {
                if (args[0].equals("cooperative")) {
                    executor = new CooperativeThreadPoolExecutor(threadCount, Runtime.getRuntime().availableProcessors());
                } else {
                    executor = Executors.newFixedThreadPool(threadCount);
                }
            }
            LOGGER.info("Running with executor type: {}, threads: {}", executor.getClass().getName(), threadCount);

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
                new CooperativeGsonHttpMessageConverter(),
                new CooperativeStringHttpMessageConverter()
        ));

        LOGGER.info("Filling the object pool...");
        objectPool = IntStream.range(0, 1000)
                .mapToObj(i -> executor.submit(() -> CooperativeThread.tryYieldFor(() -> {
                    // Manually set the accept type header to force string version of JSON
                    final HttpHeaders httpHeaders = new HttpHeaders();
                    httpHeaders.set(HttpHeaders.ACCEPT, "text/plain");
                    final RequestEntity<String> request = new RequestEntity<>(httpHeaders, HttpMethod.GET, URI.create("http://" + getNextHost() + "/"));
                    return restTemplate.exchange(request, String.class).getBody();
                })))
                .collect(Collectors.toList())
                .stream()
                .map(f -> {
                    try {
                        return f.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
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
        final OperationStatistics operationStatistics = new OperationStatistics();
        IntStream.range(0, 10_000)
                .mapToObj(i -> {
                    final Instant queueToFinishStart = Instant.now();
                    return executor.submit(() -> {
                        final Instant startToFinishStart = Instant.now();

                        final List<Supplier<NetworkResult>> getters = new ArrayList<>(2);
                        getters.add(this::mockFastNetwork);
                        getters.add(this::mockSlowNetwork);
                        Collections.shuffle(getters);

                        final NetworkResult one = getters.get(0).get();
                        final NetworkResult two = getters.get(1).get();

                        final Map<String, Object> merged = new HashMap<>();
                        merged.putAll(one.data);
                        merged.putAll(two.data);
                        final NetworkResult mergedResult = parseNetworkResult(merged);
                        mockWriteNetwork(mergedResult);

                        final Duration queueToFinishDuration = Duration.between(queueToFinishStart, Instant.now());
                        final Duration startToFinishDuration = Duration.between(startToFinishStart, Instant.now());
                        final Duration queueToStartDuration = Duration.between(queueToFinishStart, startToFinishStart);

                        progress.incrementAndGet();
                        return new OperationResult(i, queueToFinishDuration, startToFinishDuration, queueToStartDuration);
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
                .forEach(operationStatistics::add);
        final Duration totalRuntime = Duration.between(totalStart, Instant.now());

        // OK to shut these down now - no issues if they shut down a 2nd time.
        // Quiets heartbeat logging before reporting.
        executor.shutdown();
        scheduledExecutor.shutdown();

        LOGGER.info("Total runtime: {}", totalRuntime);
        LOGGER.info("{}", operationStatistics.queueToStartStats);
        LOGGER.info("{}", operationStatistics.queueToFinishStats);
        LOGGER.info("{}", operationStatistics.startToFinishStats);

        final String filename = "data-" + executor.getClass().getSimpleName() + "_" + threadCount + "-threads.csv";
        final List<OperationResult> sortedResults = operationStatistics.results.stream()
                .sorted(Comparator.comparingInt(r -> r.index))
                .collect(Collectors.toList());

        LOGGER.info("Outputting results: {}", filename);
        final CSVFormat format = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                .setDelimiter(',')
                .setQuote('"')
                .setHeader("index", "queueToFinishDuration", "startToFinishDuration")
                .build();
        try (Writer writer = new FileWriter(filename);
             CSVPrinter printer = new CSVPrinter(writer, format)) {
            for (OperationResult result : sortedResults) {
                printer.printRecord(
                        result.index,
                        result.queueToFinishDuration.toMillis(),
                        result.startToFinishDuration.toMillis());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class OperationResult {
        final int index;
        final Duration queueToFinishDuration;
        final Duration startToFinishDuration;
        final Duration queueToStartDuration;

        public OperationResult(int index, Duration queueToFinishDuration, Duration startToFinishDuration, Duration queueToStartDuration) {
            this.index = index;
            this.queueToFinishDuration = queueToFinishDuration;
            this.startToFinishDuration = startToFinishDuration;
            this.queueToStartDuration = queueToStartDuration;
        }

        @Override
        public String toString() {
            return "OperationResult{" +
                    "index=" + index +
                    ", queueToFinishDuration=" + queueToFinishDuration +
                    ", startToFinishDuration=" + startToFinishDuration +
                    ", queueToStartDuration=" + queueToStartDuration +
                    '}';
        }
    }

    private static class OperationStatistics {
        final List<OperationResult> results = new ArrayList<>();
        final StatsContainer queueToStartStats = new StatsContainer("Queue-to-start durations");
        final StatsContainer queueToFinishStats = new StatsContainer("Queue-to-finish durations");
        final StatsContainer startToFinishStats = new StatsContainer("Start-to-finish durations");

        static class StatsContainer {
            final String label;
            final DescriptiveStatistics ref;

            StatsContainer(String label) {
                this.label = label;
                this.ref = new DescriptiveStatistics();
            }

            public void addValue(double v) {
                ref.addValue(v);
            }

            @Override
            public String toString() {
                return label + " (millis): " +
                        "mean=" + Math.round(ref.getMean()) + ", " +
                        "min=" + Math.round(ref.getMin()) + ", " +
                        "max=" + Math.round(ref.getMax()) + ", " +
                        "stddev=" + ref.getStandardDeviation();
            }
        }

        void add(OperationResult r) {
            results.add(r);
            queueToStartStats.addValue(r.queueToStartDuration.toMillis());
            queueToFinishStats.addValue(r.queueToFinishDuration.toMillis());
            startToFinishStats.addValue(r.startToFinishDuration.toMillis());
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
                Thread.sleep(50L);
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