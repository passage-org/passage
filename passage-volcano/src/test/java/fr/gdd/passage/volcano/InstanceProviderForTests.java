package fr.gdd.passage.volcano;

import fr.gdd.passage.volcano.pull.PassagePullExecutor;
import fr.gdd.passage.volcano.push.PassagePushExecutor;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class InstanceProviderForTests {

    /**
     * @return A simple builder to test the push iterator model when only one scan
     *         is allowed, and parallelism is disabled.
     */
    static Stream<PassageExecutionContextBuilder<?,?>> justGo () {
        return IntStream.rangeClosed(1, 1000).mapToObj(ignored ->
                new PassageExecutionContextBuilder<>()
                        .setName("PUSH")
                        .setMaxScans(2L)
                        .setMaxParallel(2)
                        .setExecutorFactory((ec)-> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec))
        );
    }

    /**
     * @return A simple builder to test the push iterator model when only one scan
     *         is allowed, and parallelism is disabled.
     */
    static Stream<PassageExecutionContextBuilder<?,?>> oneScanOneThreadOnePush () {
        return Stream.of(new PassageExecutionContextBuilder<>()
                .setName("PUSH")
                .setMaxScans(1L)
                .setExecutorFactory((ec)-> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec)));
    }

    /**
     * @return A simple builder to test the push iterator model when only one scan
     *         is allowed, and parallelism is disabled.
     */
    static Stream<PassageExecutionContextBuilder<?,?>> oneThreadPush () {
        return Stream.of(new PassageExecutionContextBuilder<>()
                .setName("PUSH")
                .setExecutorFactory((ec)-> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec)));
    }

    /**
     * @return A simple builder to test the push iterator model when only one scan
     *         is allowed, and parallelism is disabled.
     */
    static Stream<PassageExecutionContextBuilder<?,?>> multiThreadsPush () {
        Integer[] maxParallel = {2, 5, 10};
        return Arrays.stream(maxParallel).map(p -> new PassageExecutionContextBuilder<>()
                .setName("PUSH")
                .setMaxParallel(p)
                // .setMaxScans(3L)
                .setExecutorFactory((ec) -> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec)));
    }

    /**
     * @return A stream of builder that set the execution context of Passage for
     * the *push* iterator model. We leave `setBackend` for the body of the
     * tested function.
     */
    static Stream<PassageExecutionContextBuilder<?,?>> pushProvider () {
        Long[] maxScans = {null, 1L, 2L, 3L};
        // Long[] maxScans = {2L};
        Integer[] maxParallel = {1, 2, 5, 10};

        return Arrays.stream(maxScans).flatMap(s ->
                Arrays.stream(maxParallel).map(p -> new PassageExecutionContextBuilder<>()
                        .setName("PUSH")
                        .setMaxScans(s)
                        .setMaxParallel(p)
                        .setExecutorFactory((ec) -> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec))
        ));
    }

    /**
     * @return A stream of builder that set the execution context of Passage for
     * the *pull* iterator model. We leave `setBackend` for the body of the
     * tested function.
     */
    static Stream<PassageExecutionContextBuilder<?,?>> singleThreadPushProvider() {
        Long[] maxScans = {null, 1L, 2L, 3L};
        return Arrays.stream(maxScans).map(s -> new PassageExecutionContextBuilder<>()
                        .setName("PUSH")
                        .setExecutorFactory((ec) -> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec))
                        .setMaxScans(s)
                ); // setBackend should be in the test function
    }


    /**
     * @return A stream of pull executors builder where the number of scans is limits to
     * test for pause/resumes. The `setBackend` function is left for the body of the tested function.
     */
    static Stream<PassageExecutionContextBuilder<?,?>> pullProvider () {
        Long[] maxScans = {null, 1L, 2L, 3L};
        // no support for parallel just yet.
        return Arrays.stream(maxScans).map(s -> new PassageExecutionContextBuilder<>()
                .setName("PULL")
                .setExecutorFactory((ec) -> new PassagePullExecutor<>((PassageExecutionContext<?,?>) ec))
                .setMaxScans(s)
        );
    }

}
