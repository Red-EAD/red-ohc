package com.red.ohc.api;

@FunctionalInterface
public interface DirectValueConsumer {
  /**
   * Receives a callback-scoped borrow. The supplied view, its native buffer, and any derived
   * buffer views must not be retained after this synchronous call returns.
   */
  void accept(ValueView value);
}
