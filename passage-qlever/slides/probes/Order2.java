import fr.gdd.passage.qlever.QLeverBackend;

/** Ids of several datatypes, side by side, in numeric order. */
public class Order2 {

    static long id(long datatype, long payload) { return (datatype << 60) | payload; }

    static void show(QLeverBackend b, String label, long id) {
        String v;
        try { v = b.getValue(id); } catch (Throwable t) { v = "<" + t.getClass().getSimpleName() + ">"; }
        System.out.printf("%-26s %016X  tag=%-2d  payload=%-20d  %s%n",
                label, id, id >>> 60, id & ((1L << 60) - 1), v);
    }

    public static void main(String[] args) {
        QLeverBackend b = new QLeverBackend(args[0]);

        System.out.println("--- Bool (tag 1) ---");
        show(b, "false", id(1, 0));
        show(b, "true", id(1, 1));

        System.out.println("--- Int (tag 2): the value IS the payload ---");
        for (long n : new long[]{0, 1, 2, 42, 1000}) { show(b, "integer " + n, id(2, n)); }

        System.out.println("--- Double (tag 3) ---");
        show(b, "some double", id(3, 1L << 59));

        System.out.println("--- VocabIndex (tag 4): the payload is a lookup ---");
        for (long i = 0; i < 3; i++) { show(b, "vocab entry " + i, id(4, i)); }

        System.out.println("--- the boundary ---");
        System.out.printf("largest Int Id   %016X%n", id(2, (1L << 60) - 1));
        System.out.printf("smallest Vocab Id %016X   -> every integer sorts before every IRI%n", id(4, 0));

        Runtime.getRuntime().halt(0);
    }
}
