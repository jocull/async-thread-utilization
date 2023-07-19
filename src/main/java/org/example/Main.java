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

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootApplication
public class Main implements CommandLineRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        LOGGER.info("STARTING THE APPLICATION");
        SpringApplication.run(Main.class, args);
        LOGGER.info("APPLICATION FINISHED");
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
        requestFactory.setConnectionRequestTimeout(30_000);

        final RestTemplate restTemplate = new RestTemplate(requestFactory);

        final AtomicLong lastHeartbeat = new AtomicLong(System.currentTimeMillis());
        final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            final long now = System.currentTimeMillis();
            final long then = lastHeartbeat.getAndSet(now);
            LOGGER.info("Heartbeat: " + (now - then));
        }, 0, 1, TimeUnit.SECONDS);

        final ExecutorService executor = new CooperativeThreadPoolExecutor(1000, Runtime.getRuntime().availableProcessors());
        final double result = IntStream.range(0, 100_000)
                .mapToObj(i -> executor.submit(() -> {
                    try {
                        final double before = someBusyWork();
                        final double during = CooperativeThread.yieldFor(() -> {
                            @SuppressWarnings("unchecked")
                            final Map<String, Object> data = restTemplate.getForEntity("http://localhost:3000/", Map.class).getBody();
                            return (double) data.get("randomDouble");
                        });
                        final double after = someBusyWork();
                        return before + during + after;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }))
                .collect(Collectors.toList()).stream()
                .map(f -> {
                    try {
                        return f.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                })
                .reduce(Double::sum)
                .orElseThrow();

        LOGGER.info("Result: {}", result);
        executor.shutdown();
        scheduledExecutor.shutdown();
    }

    private double someBusyWork() {
        final long target = ThreadLocalRandom.current().nextLong(1_000_000_000);
        for (long i = 0; i < target; i++) {
            // so busy
        }
        return ThreadLocalRandom.current().nextDouble();
    }
}