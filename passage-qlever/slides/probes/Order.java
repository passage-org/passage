import fr.gdd.passage.qlever.QLeverBackend;
import fr.gdd.passage.qlever.QLeverNative;

/**
 * Walk the Ids of one datatype in increasing numeric order and ask the index
 * what string each one stands for. If Id order is value order, the strings
 * come out sorted — without anything ever comparing two strings.
 */
public class Order {

    // Datatype.h: Undefined=0, Bool, Int, Double, VocabIndex=4, ...
    static final long VOCAB_INDEX = 4L;

    static long idOf(long datatype, long payload) {
        return (datatype << 60) | payload;
    }

    public static void main(String[] args) {
        QLeverBackend backend = new QLeverBackend(args[0]);
        int howMany = args.length > 1 ? Integer.parseInt(args[1]) : 13;

        System.out.printf("%-6s  %-18s  %-5s  %-12s  %s%n",
                "i", "Id (hex)", "type", "payload", "the string behind it");
        for (long i = 0; i < howMany; i++) {
            long id = idOf(VOCAB_INDEX, i);
            String value;
            try {
                value = backend.getValue(id);
            } catch (Throwable t) {
                value = "<" + t.getClass().getSimpleName() + ">";
            }
            System.out.printf("%-6d  %016X  %-5d  %-12d  %s%n",
                    i, id, id >>> 60, id & ((1L << 60) - 1), value);
        }

        Runtime.getRuntime().halt(0);
    }
}
