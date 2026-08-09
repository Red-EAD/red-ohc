package com.red.ohc.api;

/** Raised after the maintenance owner has encountered a terminal failure. */
public final class CacheMaintenanceException extends RuntimeException {
  public CacheMaintenanceException(Throwable cause) {
    super("cache maintenance failed", cause);
  }
}
