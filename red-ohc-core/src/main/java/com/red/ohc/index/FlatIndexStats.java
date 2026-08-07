package com.red.ohc.index;

/** Immutable structural snapshot of a {@link FlatConcurrentMap}. */
public final class FlatIndexStats {
    public final int size;
    public final int slotCapacity;
    public final int groups;
    public final int maxProbe;
    public final boolean resizeInProgress;
    public final long heapPayloadBytes;
    public final int overflowSize;
    public final long fallbackHeapBytes;
    public final int migrationCursor;
    public final int migratedGroups;
    public final int pendingMigrationGroups;

    FlatIndexStats(int size, int slotCapacity, int groups, int maxProbe,
                   boolean resizeInProgress, long heapPayloadBytes,
                   int overflowSize, long fallbackHeapBytes,
                   int migrationCursor, int migratedGroups, int pendingMigrationGroups) {
        this.size = size;
        this.slotCapacity = slotCapacity;
        this.groups = groups;
        this.maxProbe = maxProbe;
        this.resizeInProgress = resizeInProgress;
        this.heapPayloadBytes = heapPayloadBytes;
        this.overflowSize = overflowSize;
        this.fallbackHeapBytes = fallbackHeapBytes;
        this.migrationCursor = migrationCursor;
        this.migratedGroups = migratedGroups;
        this.pendingMigrationGroups = pendingMigrationGroups;
    }
}
