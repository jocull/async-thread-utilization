package org.example;

import org.example.cooperative.CooperativeThreadException;
import org.example.cooperative.controllers.CooperativeThreadControl;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.StringHttpMessageConverter;

import java.io.IOException;

public class CooperativeStringHttpMessageConverter extends StringHttpMessageConverter {
    private final CooperativeThreadControl control;

    public CooperativeStringHttpMessageConverter(CooperativeThreadControl control) {
        this.control = control;
    }

    @Override
    protected String readInternal(Class<? extends String> clazz, HttpInputMessage inputMessage) throws IOException {
        return control.tryRequestFor(() -> {
            try {
                return super.readInternal(clazz, inputMessage);
            } catch (Exception ex) {
                throw new CooperativeThreadException(ex);
            }
        });
    }

    @Override
    protected void writeInternal(String str, HttpOutputMessage outputMessage) throws IOException {
        control.tryRequestFor(() -> {
            try {
                super.writeInternal(str, outputMessage);
            } catch (Exception ex) {
                throw new CooperativeThreadException(ex);
            }
        });
    }
}
