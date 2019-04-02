package net.quasardb.kafka.common.config;

import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

public class ClassValidator<T> implements Validator {

    private final Class<T> clazz;

    public ClassValidator(Class<T> clazz) {
        this.clazz = clazz;
    }

    public static <T> ClassValidator impl(Class<T> clazz) {
        return new ClassValidator(clazz);
    }

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null)
            return;

        Class<?> c = (Class<?>) value;

        if (!clazz.isAssignableFrom(c))
            throw new ConfigException(name, value, "Value must implements " + clazz);
    }

}
