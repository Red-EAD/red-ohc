package com.red.ohc.storage;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CopyOnWriteArrayList;

/** Native admission budget with thread-local credits for the common small-write case. */
public final class Budget {
    private static final long CREDIT_BATCH = 64L << 10;
    private static final long LEDGER_CHUNK_BYTES = 24L * 256L;
    private static final long MIN_LEDGER_BYTES = LEDGER_CHUNK_BYTES * 2L;
    private final long capacity;
    private final AtomicLong reserved = new AtomicLong();
    private final AtomicLong ledgerBytes = new AtomicLong();
    private final CopyOnWriteArrayList<Credit> allCredits = new CopyOnWriteArrayList<>();
    private final ThreadLocal<Credit> credits = ThreadLocal.withInitial(() -> {
        Credit credit = new Credit();
        allCredits.add(credit);
        return credit;
    });

    public Budget(long capacity) {
        this.capacity = capacity;
    }

    public boolean reserve(long bytes) {
        if (bytes <= 0L || bytes > capacity) return false;
        Credit credit = credits.get();
        if (credit.bytes >= bytes) {
            credit.bytes -= bytes;
            return true;
        }
        long doubled = bytes > Long.MAX_VALUE / 2L ? Long.MAX_VALUE : bytes * 2L;
        long batch = Math.min(capacity, Math.max(bytes, Math.min(CREDIT_BATCH, doubled)));
        for (;;) {
            long current = reserved.get();
            if (current > capacity - batch) return false;
            if (reserved.compareAndSet(current, current + batch)) {
                credit.bytes += batch - bytes;
                return true;
            }
        }
    }

    public void release(long bytes) {
        if (bytes > 0L) reserved.addAndGet(-bytes);
    }

    /**
     * Returns a reservation that was never published as native storage to the
     * calling thread's credit. The global reservation still includes that
     * credit, so using release here would make the budget under-account it.
     */
    public void refund(long bytes) {
        if (bytes <= 0L) return;
        Credit credit = credits.get();
        credit.bytes += bytes;
    }

    /** Reserves bounded maintenance-ledger storage separately from live payload admission. */
    public boolean reserveLedger(long bytes) {
        if (bytes <= 0L) return false;
        long limit = Math.max(capacity, MIN_LEDGER_BYTES);
        for (;;) {
            long current = ledgerBytes.get();
            if (current > limit - bytes) return false;
            if (ledgerBytes.compareAndSet(current, current + bytes)) return true;
        }
    }

    public void releaseLedger(long bytes) {
        if (bytes > 0L) ledgerBytes.addAndGet(-bytes);
    }

    public long ledgerBytes() {
        return ledgerBytes.get();
    }

    public long reserved() {
        return reserved.get();
    }

    public long capacity() {
        return capacity;
    }

    /** Returns unused thread-local reservations when the cache is closed. */
    public void returnUnusedCredits() {
        for (Credit credit : allCredits) {
            long unused = credit.bytes;
            credit.bytes = 0L;
            if (unused > 0L) reserved.addAndGet(-unused);
        }
    }

    public void clear() {
        for (Credit credit : allCredits) credit.bytes = 0L;
        reserved.set(0L);
        ledgerBytes.set(0L);
    }

    private static final class Credit {
        long bytes;
    }
}
