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

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootApplication
public class Main implements CommandLineRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private RestTemplate restTemplate;

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

        restTemplate = new RestTemplate(requestFactory);

        final AtomicLong lastHeartbeat = new AtomicLong(System.currentTimeMillis());
        final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            final long now = System.currentTimeMillis();
            final long then = lastHeartbeat.getAndSet(now);
            LOGGER.info("Heartbeat: " + (now - then));
        }, 0, 1, TimeUnit.SECONDS);

        final ExecutorService executor = new CooperativeThreadPoolExecutor(1000, Runtime.getRuntime().availableProcessors() * 2);
        // final ExecutorService executor = Executors.newFixedThreadPool(1000);

        final double result = IntStream.range(0, 100_000)
                .mapToObj(i -> executor.submit(() -> {
                    try {
                        final double before = someBusyNetWork();
                        final double during = slowNetwork();
                        final double after = someBusyNetWork();
                        return before + during + after;
                    } catch (Exception e) {
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

    private double someBusyCpuWork() {
        final long target = ThreadLocalRandom.current().nextLong(50_000_000L);
        for (long i = 0; i < target; i++) {
            // so busy
        }
        return ThreadLocalRandom.current().nextDouble();
    }

    private double someBusyNetWork() throws InterruptedException {
        final long target = ThreadLocalRandom.current().nextLong(100);
        double sum = 0;
        for (long i = 0; i < target; i++) {
            sum += someBusyCpuWork();
            sum += fastNetwork();
            sum += someBusyCpuWork();
        }
        return sum;
    }

    @SuppressWarnings("unchecked")
    private double fastNetwork() throws InterruptedException {
        return CooperativeThread.tryYieldFor(() -> {
            final Map<String, Object> data = restTemplate.getForEntity("http://localhost:3000/", Map.class).getBody();
            return (double) data.get("randomDouble");
        });
    }

    @SuppressWarnings("unchecked")
    private double slowNetwork() throws InterruptedException {
        return CooperativeThread.tryYieldFor(() -> {
            final Map<String, Object> data = restTemplate.getForEntity("http://localhost:3000/slow", Map.class).getBody();
            return (double) data.get("randomDouble");
        });
    }
}