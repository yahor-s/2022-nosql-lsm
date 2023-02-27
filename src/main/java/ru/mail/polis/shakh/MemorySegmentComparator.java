package ru.mail.polis.shakh;

import java.util.Comparator;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

public class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        long pos = o1.mismatch(o2);
        if (pos < 0) {
            return 0;
        }
        if (pos == o1.byteSize()) {
            return -1;
        }
        if (pos == o2.byteSize()) {
            return 1;
        }
        return Byte.compare(
            MemoryAccess.getByteAtOffset(o1, pos),
            MemoryAccess.getByteAtOffset(o2, pos));
    }
}
