package org.example;

import org.example.executor.CooperativeThread;
import org.springframework.http.converter.json.GsonHttpMessageConverter;

import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;

public class CooperativeGsonHttpMessageConverter extends GsonHttpMessageConverter {
    @Override
    protected Object readInternal(Type resolvedType, Reader reader) throws Exception {
        return CooperativeThread.tryRequestFor(() -> {
            try {
                return super.readInternal(resolvedType, reader);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    protected void writeInternal(Object object, Type type, Writer writer) throws Exception {
        CooperativeThread.tryRequestFor(() -> {
            try {
                super.writeInternal(object, type, writer);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
