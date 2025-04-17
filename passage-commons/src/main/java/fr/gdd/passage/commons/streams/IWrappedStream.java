package fr.gdd.passage.commons.streams;

import java.util.stream.Stream;

public interface IWrappedStream<T> {

    Stream<T> stream();

}
