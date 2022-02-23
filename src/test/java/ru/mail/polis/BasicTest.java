package ru.mail.polis;

import org.junit.jupiter.api.Assertions;
import org.opentest4j.AssertionFailedError;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author incubos
 */
public class BasicTest extends BaseTest {

    @DaoTest
    void testEmpty(Dao<String, Entry<String>> dao) {
        assertEmpty(dao.all());
    }

    @DaoTest
    void testSingle(Dao<String, Entry<String>> dao) {
        dao.upsert(entry("a", "b"));
        assertSame(
                dao.all(),
                entry("a", "b")
        );
    }

    @DaoTest
    void testTree(Dao<String, Entry<String>> dao) {
        dao.upsert(entry("e", "f"));
        dao.upsert(entry("c", "d"));
        dao.upsert(entry("a", "b"));

        assertSame(
                dao.all(),
                entry("a", "b"),
                entry("c", "d"),
                entry("e", "f")
        );
    }

    @DaoTest
    void testManyIterators(Dao<String, Entry<String>> dao) throws Exception {
        List<Entry<String>> entries = new ArrayList<>(entries("k", "v", 10_000));
        for (Entry<String> entry : entries) {
            dao.upsert(entry);
        }
        try {
            List<Iterator<Entry<String>>> iterators = new ArrayList<>();
            for (int i = 0; i < 10_000; i++) {
                iterators.add(dao.all());
            }
            // just utilize the collection
            Assertions.assertEquals(10_000, iterators.size());
        } catch (OutOfMemoryError error) {
            throw new AssertionFailedError("Too much data in memory: use some lazy ways", error);
        }
    }


}
