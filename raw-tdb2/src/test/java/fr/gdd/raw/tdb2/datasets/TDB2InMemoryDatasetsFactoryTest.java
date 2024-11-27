package fr.gdd.raw.tdb2.datasets;

import org.junit.jupiter.api.Test;

class TDB2InMemoryDatasetsFactoryTest {

    @Test
    public void in_jena_create_of_small_inmemory_tdb2_datasets_for_test () {
        // Assertions.assertDoesNotThrow( () -> {
        var ignored = TDB2InMemoryDatasetsFactory.triple3();
        ignored = TDB2InMemoryDatasetsFactory.triple6();
        ignored = TDB2InMemoryDatasetsFactory.triple9();
        //ignored = IM4Jena.graph3();
        ignored = TDB2InMemoryDatasetsFactory.triples9PlusLiterals();
        // ignored = IM4Jena.stars();
        //});
    }


//    @Test
//    @EnabledIfEnvironmentVariable(named = "WATDIV", matches = "true")
//    public void download_extract_ingest_clean() {
//        Path testingPath = Path.of("datasets/watdiv-test");
//        try {
//            FileUtils.deleteDirectory(testingPath.toFile());
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        assertFalse(testingPath.toFile().exists()); // start the test anew
//
//        Watdiv10M watdiv10M = new Watdiv10M(Optional.of(testingPath.toString()));
//        assertTrue(testingPath.toFile().exists());
//        assertTrue(new File(watdiv10M.dbPath_asStr).exists());
//        assertFalse(watdiv10M.pathToArchive.toFile().exists());
//        assertFalse(watdiv10M.fullExtractPath.toFile().exists());
//
//        // cleanup test
//        try {
//            FileUtils.deleteDirectory(testingPath.toFile());
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

}