package org.example;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.example.executor.CooperativeThread;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootApplication
public class Main implements CommandLineRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private RestTemplate restTemplate;
    static final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    // static final ExecutorService executor = new CooperativeThreadPoolExecutor(1000, Runtime.getRuntime().availableProcessors() * 2);
    static final ExecutorService executor = Executors.newFixedThreadPool(1000);

    public static void main(String[] args) {
        try {
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
        final HttpClient httpClient = HttpClientBuilder.create()
                .setMaxConnPerRoute(200)
                .setMaxConnTotal(200)
                .build();

        final HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory(httpClient);
        requestFactory.setConnectTimeout(5_000);
        requestFactory.setReadTimeout(5_000);
        requestFactory.setConnectionRequestTimeout(0);

        restTemplate = new RestTemplate(requestFactory);
        restTemplate.setMessageConverters(List.of(
                new CooperativeGsonHttpMessageConverter()
        ));

        final AtomicInteger progress = new AtomicInteger();
        final AtomicLong lastHeartbeat = new AtomicLong(System.currentTimeMillis());
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            final long now = System.currentTimeMillis();
            final long then = lastHeartbeat.getAndSet(now);
            LOGGER.info("Heartbeat: " + (now - then) + " @ " + progress.get());
        }, 0, 1, TimeUnit.SECONDS);

        final Instant totalStart = Instant.now();
        final List<OperationResult> result = IntStream.range(0, 10_000)
                .mapToObj(i -> {
                    // TODO: Measure outer + inner time
                    return executor.submit(() -> {
                        final Instant start = Instant.now();
                        if (i % 2 == 0) {
                            fastNetwork();
                            slowNetwork();
                        } else {
                            slowNetwork();
                            fastNetwork();
                        }
                        final double localResult = ThreadLocalRandom.current().nextDouble();
                        final Duration duration = Duration.between(start, Instant.now());

                        progress.incrementAndGet();
                        return new OperationResult(i, duration, localResult);
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

        result.forEach(x -> LOGGER.info("Result: {}; {}", x.index, x.duration));
        LOGGER.info("Total runtime: {}", totalRuntime);
    }

    private static class OperationResult {
        final int index;
        final Duration duration;
        final double result;

        public OperationResult(int index, Duration duration, double result) {
            this.index = index;
            this.duration = duration;
            this.result = result;
        }
    }

    private double someBusyCpuWork() {
        final long target = ThreadLocalRandom.current().nextLong(50_000_000L);
        for (long i = 0; i < target; i++) {
            // so busy
        }
        return ThreadLocalRandom.current().nextDouble();
    }

    private double someBusyNetWork() {
        final long target = ThreadLocalRandom.current().nextLong(100);
        double sum = 0;
        for (long i = 0; i < target; i++) {
//            sum += someBusyCpuWork();
            sum += fastNetwork();
//            sum += someBusyCpuWork();
        }
        return sum;
    }

    @SuppressWarnings("unchecked")
    private double fastNetwork() {
        return CooperativeThread.tryYieldFor(() -> {
            final Map<String, Object> data = restTemplate.getForEntity("http://localhost:3000/", Map.class).getBody();
            return (double) data.get("randomDouble");
        });
    }

    @SuppressWarnings("unchecked")
    private double slowNetwork() {
        return CooperativeThread.tryYieldFor(() -> {
            final Map<String, Object> data = restTemplate.getForEntity("http://localhost:3000/slow", Map.class).getBody();
            return (double) data.get("randomDouble");
        });
    }
}