package fr.gdd.passage.commons.streams;

import java.util.stream.Stream;

public class WrappedStreamSingleton<T> implements IWrappedStream<T> {

    final T value;

    public WrappedStreamSingleton(T value) {
        this.value = value;
    }

    @Override
    public Stream<T> stream() {
        return Stream.of(value);
    }
}
