package com.red.ohc;

@FunctionalInterface
public interface DirectValueConsumer {
    void accept(ValueView value);
}
