package com.red.ohc.runtime;

import com.red.ohc.index.Entry;

@FunctionalInterface
public interface AccessConsumer {
  void accept(
      Entry entry, long observedValueAddress, long observedGeneration, int observedPolicyState);
}
