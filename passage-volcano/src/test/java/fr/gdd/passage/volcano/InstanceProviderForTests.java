package fr.gdd.passage.volcano;

import java.util.Arrays;
import java.util.stream.Stream;

public class InstanceProviderForTests {

    /**
     * @return A stream of builder that set the execution context of Passage for
     * the *push* iterator model. We leave `setBackend` for the body of the
     * tested function.
     */
    static Stream<PassageExecutionContextBuilder> pushProvider () {
        Long[] maxScans = {null, 1L, 2L, 3L};
        // Long[] maxScans = {2L};
        Integer[] maxParallel = {1, 2, 5, 10};

        return Arrays.stream(maxScans).flatMap(s ->
                Arrays.stream(maxParallel).map(p -> new PassageExecutionContextBuilder()
                        .setMaxScans(s)
                        .setMaxParallel(p)
        ));
    }

    /**
     * @return A stream of builder that set the execution context of Passage for
     * the *pull* iterator model. We leave `setBackend` for the body of the
     * tested function.
     */
    static Stream<PassageExecutionContextBuilder> singleThreadProvider () {
        Long[] maxScans = {null, 1L, 2L, 3L};
        // no support for parallel just yet.
        return Arrays.stream(maxScans).map(s -> new PassageExecutionContextBuilder()
                        .setMaxScans(s)
                ); // setBackend should be in the test function
    }


}
