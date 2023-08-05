package org.example;

import org.example.cooperative.CooperativeThreadException;
import org.example.cooperative.controllers.CooperativeThreadControl;
import org.springframework.http.converter.json.GsonHttpMessageConverter;

import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;

public class CooperativeGsonHttpMessageConverter extends GsonHttpMessageConverter {
    private final CooperativeThreadControl control;

    public CooperativeGsonHttpMessageConverter(CooperativeThreadControl control) {
        this.control = control;
    }

    @Override
    protected Object readInternal(Type resolvedType, Reader reader) throws Exception {
        return control.tryRequestFor(() -> {
            try {
                return super.readInternal(resolvedType, reader);
            } catch (Exception ex) {
                throw new CooperativeThreadException(ex);
            }
        });
    }

    @Override
    protected void writeInternal(Object object, Type type, Writer writer) throws Exception {
        control.tryRequestFor(() -> {
            try {
                super.writeInternal(object, type, writer);
            } catch (Exception ex) {
                throw new CooperativeThreadException(ex);
            }
        });
    }
}
