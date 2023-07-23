package org.example;

import org.example.executor.CooperativeThread;
import org.example.executor.CooperativeThreadException;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.StringHttpMessageConverter;

import java.io.IOException;

public class CooperativeStringHttpMessageConverter extends StringHttpMessageConverter {
    @Override
    protected String readInternal(Class<? extends String> clazz, HttpInputMessage inputMessage) throws IOException {
        return CooperativeThread.tryRequestFor(() -> {
            try {
                return super.readInternal(clazz, inputMessage);
            } catch (Exception ex) {
                throw new CooperativeThreadException(ex);
            }
        });
    }

    @Override
    protected void writeInternal(String str, HttpOutputMessage outputMessage) throws IOException {
        CooperativeThread.tryRequestFor(() -> {
            try {
                super.writeInternal(str, outputMessage);
            } catch (Exception ex) {
                throw new CooperativeThreadException(ex);
            }
        });
    }
}
