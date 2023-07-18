package org.example;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.example.executor.SemaphoreThread;
import org.example.executor.SemaphoreThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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

        final ExecutorService executor = new SemaphoreThreadPoolExecutor(1000, Runtime.getRuntime().availableProcessors());

        final double result = IntStream.range(0, 100_000)
                .mapToObj(i -> executor.submit(() -> {
                    try {
                        @SuppressWarnings("unchecked") final Map<String, Object> data = SemaphoreThread.releaseForOperation(() ->
                                restTemplate.getForEntity("http://localhost:3000/", Map.class).getBody());

                        return (double) data.get("randomDouble");
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
    }
}