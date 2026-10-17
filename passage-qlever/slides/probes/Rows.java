import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.qlever.QLeverBackend;
import fr.gdd.passage.qlever.QLeverNative;

/**
 * The first rows of a PSO scan, as they sit in a block: col0 = predicate,
 * col1 = subject, col2 = object. Printed as Ids, which is what is stored.
 */
public class Rows {
    public static void main(String[] args) {
        QLeverBackend b = new QLeverBackend(args[0]);
        long any = QLeverNative.any();
        long p = b.getId("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
        int howMany = args.length > 1 ? Integer.parseInt(args[1]) : 10;

        BackendIterator<Long, String> it = b.search(any, p, any);
        System.out.printf("%-4s  %-18s %-18s %-18s%n", "row", "col0 (P)", "col1 (S)", "col2 (O)");
        for (int i = 0; i < howMany && it.hasNext(); i++) {
            it.next();
            System.out.printf("%-4d  %016X   %016X   %016X%n", i,
                    it.getId(SPOC.PREDICATE), it.getId(SPOC.SUBJECT), it.getId(SPOC.OBJECT));
        }
        System.out.println();
        it.skip(0);
        it.next();
        System.out.println("row 0 spelled out:");
        System.out.printf("   col0  %s%n", it.getValue(SPOC.PREDICATE));
        System.out.printf("   col1  %s%n", it.getValue(SPOC.SUBJECT));
        System.out.printf("   col2  %s%n", it.getValue(SPOC.OBJECT));

        Runtime.getRuntime().halt(0);
    }
}
