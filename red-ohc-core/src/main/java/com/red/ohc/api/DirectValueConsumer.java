package com.red.ohc.api;

@FunctionalInterface
public interface DirectValueConsumer {
  void accept(ValueView value);
}
