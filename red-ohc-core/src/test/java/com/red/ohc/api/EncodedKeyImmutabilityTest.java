package com.red.ohc.api;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public final class EncodedKeyImmutabilityTest {
  @Test
  public void byteAccessCannotMutateThePrecomputedKey() {
    byte[] source = {1, 2, 3, 4};
    EncodedKey key = EncodedKey.copyOf(source);
    int expectedHash = key.hash();

    source[0] = 9;
    byte[] exposed = key.bytes();
    exposed[1] = 8;

    assertEquals(key.bytes()[0], (byte) 1);
    assertEquals(key.bytes()[1], (byte) 2);
    assertEquals(key.hash(), expectedHash);
  }

  @Test
  public void copyToCopiesBytesWithoutExposingThePrecomputedKey() {
    EncodedKey key = EncodedKey.copyOf(new byte[] {1, 2, 3, 4});
    byte[] target = new byte[8];

    key.copyTo(target, 2);
    target[2] = 9;

    assertEquals(target[3], (byte) 2);
    assertEquals(key.bytes()[0], (byte) 1);
    assertEquals(key.hash(), EncodedKey.copyOf(new byte[] {1, 2, 3, 4}).hash());
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void copyToRejectsAnInsufficientTarget() {
    EncodedKey.copyOf(new byte[] {1, 2, 3, 4}).copyTo(new byte[3], 0);
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void copyToRejectsANegativeOffset() {
    EncodedKey.copyOf(new byte[] {1, 2, 3, 4}).copyTo(new byte[8], -1);
  }
}
