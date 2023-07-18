package org.example;

import com.google.gson.Gson;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@SpringBootApplication
public class Main implements CommandLineRunner {
    private static Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        LOGGER.info("STARTING THE APPLICATION");
        SpringApplication.run(Main.class, args);
        LOGGER.info("APPLICATION FINISHED");
    }

    @Override
    public void run(String... args) throws Exception {
        final HttpClient httpClient = HttpClientBuilder.create()
                .setMaxConnPerRoute(75)
                .setMaxConnTotal(200)
                .build();

        final HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory(httpClient);
        requestFactory.setConnectTimeout(5_000);
        requestFactory.setReadTimeout(5_000);
        requestFactory.setConnectionRequestTimeout(5_000);

        final RestTemplate restTemplate = new RestTemplate(requestFactory);
        final Gson gson = new Gson();

        @SuppressWarnings("unchecked")
        final Map<String, Object> data = restTemplate.getForEntity("http://localhost:3000/", Map.class).getBody();
    }
}