package org.example;

import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.example.cooperative.CooperativeThreadInterruptedException;
import org.example.cooperative.controllers.CooperativeThreadControl;
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
    static int threadCount = 1000;
    static int parallelism = -1;
    static boolean isCooperative = false;
    static CooperativeThreadControl control;
    static final List<String> hosts = List.of("localhost:3000");
    static final Iterator<String> hostIterator = Iterators.cycle(hosts);
    static List<String> objectPool;
    static ThreadLocal<Iterator<String>> objectPoolIterator = new ThreadLocal<>();

    public static void main(String[] args) {
        try {
            if (args.length > 2) {
                try {
                    parallelism = Integer.parseInt(args[2]);
                } catch (Exception ex) {
                    LOGGER.error("Can't parse parallelism", ex);
                }
            }
            if (parallelism < 1) {
                parallelism = Runtime.getRuntime().availableProcessors();
            }

            if (args.length > 1) {
                try {
                    threadCount = Integer.parseInt(args[1]);
                } catch (Exception ex) {
                    LOGGER.error("Can't parse thread count", ex);
                }
            }

            executor = Executors.newFixedThreadPool(threadCount);
            if (args.length > 0 && args[0].equals("cooperative")) {
                control = CooperativeThreadControl.create(parallelism);
                isCooperative = true;
            }
            if (control == null) {
                parallelism = threadCount;
                control = CooperativeThreadControl.none();
            }
            LOGGER.info("Running with control type: {}, threads: {}, parallelism: {}", control.getClass().getName(), threadCount, parallelism);

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
                new CooperativeGsonHttpMessageConverter(control),
                new CooperativeStringHttpMessageConverter(control)
        ));

        LOGGER.info("Filling the object pool...");
        objectPool = IntStream.range(0, 1000)
                .mapToObj(i -> executor.submit(() -> control.runTask(() -> {
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

        // Drop all but the largest so runs can be better compared
        objectPool = List.of(
                objectPool.stream().max(Comparator.comparingInt(String::length)).orElseThrow()
        );
        LOGGER.info("Done - using item of length {}", objectPool.get(0).length());

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
                    final Instant queueTime = Instant.now();
                    return executor.submit(() -> control.runTask(() -> {
                        final Instant startTime = Instant.now();

                        final List<NetworkResult> networkResults = mockJitteryNetwork();
                        final Map<String, Object> merged = new HashMap<>();
                        networkResults.forEach(nr -> merged.putAll(nr.data));

                        final NetworkResult mergedResult = parseNetworkResult(merged);
                        mockWriteNetwork(mergedResult);

                        final Instant finishTime = Instant.now();
                        progress.incrementAndGet();
                        return new OperationResult(i, queueTime, startTime, finishTime);
                    }));
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

        final String filename = isCooperative
                ? "data-" + control.getClass().getSimpleName() + "_" + threadCount + "-" + parallelism +  "-threads.csv"
                : "data-" + control.getClass().getSimpleName() + "_" + threadCount + "-threads.csv";
        final List<OperationResult> sortedResults = operationStatistics.results.stream()
                .sorted(Comparator.comparingInt(r -> r.index))
                .collect(Collectors.toList());

        LOGGER.info("Outputting results: {}", filename);
        final CSVFormat format = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                .setDelimiter(',')
                .setQuote('"')
                .setHeader("index", "queueTime", "startTime", "finishTime", "queueToStartDuration", "queueToFinishDuration", "startToFinishDuration")
                .build();
        try (Writer writer = new FileWriter(filename);
             CSVPrinter printer = new CSVPrinter(writer, format)) {
            for (OperationResult result : sortedResults) {
                printer.printRecord(
                        result.index,
                        result.queueTime.toEpochMilli(),
                        result.startTime.toEpochMilli(),
                        result.finishTime.toEpochMilli(),
                        result.queueToStartDuration.toMillis(),
                        result.queueToFinishDuration.toMillis(),
                        result.startToFinishDuration.toMillis());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class OperationResult {
        final int index;
        final Instant queueTime;
        final Instant startTime;
        final Instant finishTime;
        final Duration queueToFinishDuration;
        final Duration startToFinishDuration;
        final Duration queueToStartDuration;

        public OperationResult(int index, Instant queueTime, Instant startTime, Instant finishTime) {
            this.index = index;
            this.queueTime = queueTime;
            this.startTime = startTime;
            this.finishTime = finishTime;
            this.queueToFinishDuration = Duration.between(queueTime, finishTime);
            this.startToFinishDuration = Duration.between(startTime, finishTime);
            this.queueToStartDuration = Duration.between(queueTime, startTime);
        }

        @Override
        public String toString() {
            return "OperationResult{" +
                    "index=" + index +
                    ", queueTime=" + queueTime +
                    ", startTime=" + startTime +
                    ", finishTime=" + finishTime +
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

    private String nextObject() {
        Iterator<String> cycle = objectPoolIterator.get();
        if (cycle == null) {
            cycle = Iterators.cycle(objectPool);
            objectPoolIterator.set(cycle);
        }
        return cycle.next();
    }

    @SuppressWarnings("unchecked")
    private NetworkResult mockFastNetwork() {
        final String data = nextObject();
        return parseNetworkResult(new Gson().fromJson(data, Map.class));
    }

    private NetworkResult mockSlowNetwork() {
        control.tryYieldFor(() -> {
            try {
                Thread.sleep(50L);
            } catch (InterruptedException ex) {
                throw new CooperativeThreadInterruptedException(ex);
            }
        });
        return mockFastNetwork();
    }

    private List<NetworkResult> mockJitteryNetwork() {
        return IntStream.range(0, 10)
                .mapToObj(i -> {
                    control.tryYieldFor(() -> {
                        try {
                            Thread.sleep(ThreadLocalRandom.current().nextLong(10, 20));
                        } catch (InterruptedException ex) {
                            throw new CooperativeThreadInterruptedException(ex);
                        }
                    });
                    return mockFastNetwork();
                })
                .collect(Collectors.toList());
    }

    private void writeNetwork(NetworkResult networkResult) {
        control.tryYieldFor(() -> restTemplate.postForEntity("http://" + getNextHost() + "/", networkResult.data, Void.class));
    }

    private void mockWriteNetwork(NetworkResult networkResult) {
        final String data = new Gson().toJson(networkResult.data);
        if (data.length() == 0) {
            throw new IllegalArgumentException("Data not serialized as expected");
        }
    }

    @SuppressWarnings("unchecked")
    private NetworkResult runNetwork(String url) {
        final Map<String, Object> data = control.tryYieldFor(() -> restTemplate.getForEntity(url, Map.class).getBody());
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