import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.qlever.QLeverBackend;
import fr.gdd.passage.qlever.QLeverNative;

/**
 * What an augmented B-tree gives in O(log n) and this store does not: select by
 * offset, and an exact range count.
 *
 * <p>NOTE that `skip` is lazy — it records an offset and repositions on the
 * next read — so every timing here reads a component afterwards. Timing `skip`
 * alone, or at a fixed offset, measures nothing.
 */
public class Limits {

    static long nanosPer(Runnable r, int times) {
        for (int i = 0; i < Math.min(times, 30); i++) { r.run(); }   // let the JIT settle
        long start = System.nanoTime();
        for (int i = 0; i < times; i++) { r.run(); }
        return (System.nanoTime() - start) / times;
    }

    /** Re-enter and read, at offsets that advance — what a continuation does. */
    static long forward(BackendIterator it, long card, int times) {
        final long[] i = {0};
        long stride = Math.max(1, card / (times * 4L));
        return nanosPer(() -> {
            it.skip((i[0]++ * stride) % card);
            if (it.hasNext()) { it.next(); it.getId(0); }
        }, times);
    }

    /** Re-enter and read, at offsets that jump — what a random walk does. */
    static long scattered(BackendIterator it, long card, int times) {
        java.util.Random rng = new java.util.Random(42);
        return nanosPer(() -> {
            it.skip(Math.floorMod(rng.nextLong(), card));
            if (it.hasNext()) { it.next(); it.getId(0); }
        }, times);
    }

    public static void main(String[] args) throws Exception {
        Backend backend;
        if (args[0].endsWith(".jnl")) {
            backend = new BlazegraphBackend(args[0]);
            System.out.println("Blazegraph");
        } else {
            backend = new QLeverBackend(args[0]);
            QLeverNative.setBlockCacheSize(args.length > 1 ? Long.parseLong(args[1]) : 0);
            System.out.printf("QLever, block cache = %d blocks%n",
                    QLeverNative.getBlockCacheSize());
        }
        Object any = backend.any();

        String type = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
        BackendIterator[] shapes = new BackendIterator[]{
                backend.search(any, any, any),
                backend.search(any, backend.getId(type), any)
        };
        String[] labels = {"?s ?p ?o", "?s a ?c "};

        for (int k = 0; k < shapes.length; k++) {
            BackendIterator it = shapes[k];
            long card = (long) it.cardinality();
            long truth = 0;
            for (BackendIterator probe = (k == 0)
                    ? backend.search(any, any, any)
                    : backend.search(any, backend.getId(type), any);
                 probe.hasNext(); ) { probe.next(); truth++; }
            System.out.printf("%n%s   cardinality() says %,d   ·   counted %,d%s%n",
                    labels[k], card, truth,
                    card == truth ? "   (exact)" : "   <<< NOT EXACT");
            System.out.printf("   cardinality() exact        %,9d ns%n",
                    nanosPer(it::cardinality, 100));
            try {
                System.out.printf("   cardinality() estimate     %,9d ns%n",
                        nanosPer(() -> it.cardinality(1L), 100));
            } catch (Throwable t) {
                System.out.printf("   cardinality() estimate     unsupported (%s)%n",
                        t.getClass().getSimpleName());
            }
            System.out.printf("   re-enter forward + read    %,9d ns%n",
                    forward(it, card, 100));
            System.out.printf("   re-enter scattered + read  %,9d ns%n",
                    scattered(it, card, 100));
            System.out.printf("   random() one uniform draw  %,9d ns%n",
                    nanosPer(it::random, 100));
        }

        backend.close();
        Runtime.getRuntime().halt(0);
    }
}
