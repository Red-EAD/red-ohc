package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.testng.annotations.Test;

import com.red.ohc.AllocatorType;

public class PageIdReuseTest {
    @Test
    public void physicallyFreedPageReusesItsSparseIdWithANewVersionStamp() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        WriterArena.Page first = memory.acquireEntryPage(0);
        try {
            int id = first.id;
            int pageKey = first.pageKey;
            memory.returnUnusedPage(first);

            WriterArena.Page second = memory.acquireEntryPage(0);
            try {
                assertEquals(second.id, id);
                assertNotEquals(second.pageKey, pageKey,
                        "a reused sparse id must reject stale allocator handles from the old page");
            } finally {
                memory.returnUnusedPage(second);
            }
        } finally {
            memory.closeArenas();
        }
    }
}
