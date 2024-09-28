package fr.gdd.raw.tdb2;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.tdb2.TDB2Factory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Creates an artifical graph where the cardinality of the groups are different (there are
 * a lot more small groups that big groups), and have different probability to appear.
 */
public class ArtificallySkewedGraph {

    Dataset dataset;
    public static Random rng = new Random(12);

    public ArtificallySkewedGraph(Integer distinct, Integer probaRange) {
        dataset = TDB2Factory.createDataset();
        dataset.begin(ReadWrite.WRITE);

        List<String> statements = new ArrayList<>();
        for (int i = 0; i < distinct; ++i) {
            statements.add(String.format("<http://prof_%s> <http://is_a> <http://Prof> .", i));
            Double meow = rng.nextGaussian();
            Integer students = rng.nextInt(1, ((int) (Math.abs(meow * probaRange)) )+ 2);
            for (int j = 0; j < students; ++j) {
                statements.add(String.format("<http://prof_%s> <http://teaches> <http://student_%s_%s> .", i, i, j));
                if (j % 2 == 0) {
                    statements.add(String.format("<http://student_%s_%s> <http://belongs_to> <http://group_%s> .", i, j, i));
                }
            }
        }

        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model model = ModelFactory.createDefaultModel();
        model.read(statementsStream, "", Lang.NT.getLabel());
        dataset.setDefaultModel(model);
        dataset.commit();
        dataset.close();
    }

    public Dataset getDataset() {
        return dataset;
    }
}
