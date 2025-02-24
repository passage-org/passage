package fr.gdd.passage.blazegraph;

import fr.gdd.passage.commons.generics.BackendManager;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class BlazegraphBackendManagerTest {

    private final static String path2watdiv = "/Users/nedelec-b-2/Desktop/Projects/temp/watdiv10m-blaze/watdiv10M.properties";
    private final static String path2wdbench = "/Users/nedelec-b-2/Desktop/Projects/temp/wdbench-blaze/wdbench-blaze.jnl";

    @Test
    public void create_the_manager_with_a_same_dataset_path () throws Exception {
        BackendManager manager = new BackendManager();
        manager.addBackend(path2watdiv, new BlazegraphBackendFactory());
        manager.addBackend(path2watdiv, new BlazegraphBackendFactory());
        Backend bb = manager.addBackend(path2watdiv, new BlazegraphBackendFactory());

        BackendIterator it = bb.search(bb.any(), bb.any(), bb.any(), bb.any());
        assertTrue(it.hasNext());
        assertEquals(1, manager.size());

        manager.close();
        assertEquals(0, manager.size());
    }

    @Test
    public void create_manager_with_two_dataset_paths () throws Exception {
        BackendManager manager = new BackendManager();
        manager.addBackend(path2watdiv, new BlazegraphBackendFactory());
        manager.addBackend(path2wdbench, new BlazegraphBackendFactory());
        assertEquals(2, manager.size());

        manager.close();
        assertEquals(0, manager.size());
    }

}
