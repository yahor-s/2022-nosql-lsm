package ru.mail.polis.shakh;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> daoMap = new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException {
        if (from == null && to == null) {
            return daoMap.values().iterator();
        } else if (from == null) {
            return daoMap.headMap(to).values().iterator();
        } else if (to == null) {
            return daoMap.tailMap(from, true).values().iterator();
        }
        return daoMap.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        daoMap.put(entry.key(), entry);
    }

}
