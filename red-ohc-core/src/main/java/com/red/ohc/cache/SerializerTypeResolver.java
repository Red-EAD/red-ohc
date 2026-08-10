package com.red.ohc.cache;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.red.ohc.api.CacheSerializer;

/** Reflective guard for the directly identifiable ByteBuffer value serializer types. */
final class SerializerTypeResolver {
  private SerializerTypeResolver() {}

  static boolean resolvesToByteBuffer(CacheSerializer<?> serializer) {
    return resolvesToByteBuffer(serializer.getClass(), new HashMap<>());
  }

  private static boolean resolvesToByteBuffer(Type type, Map<Type, Type> bindings) {
    if (type instanceof Class<?>) {
      Class<?> raw = (Class<?>) type;
      for (Type interfaceType : raw.getGenericInterfaces()) {
        if (resolvesToByteBuffer(interfaceType, new HashMap<>(bindings))) {
          return true;
        }
      }
      Type superclass = raw.getGenericSuperclass();
      return resolvesToByteBuffer(superclass, new HashMap<>(bindings));
    }
    if (!(type instanceof ParameterizedType)) {
      return false;
    }
    ParameterizedType parameterized = (ParameterizedType) type;
    Type rawType = parameterized.getRawType();
    if (rawType == CacheSerializer.class) {
      Type valueType = resolve(parameterized.getActualTypeArguments()[0], bindings);
      return valueType instanceof Class<?> && ByteBuffer.class.isAssignableFrom((Class<?>) valueType);
    }
    if (!(rawType instanceof Class<?>)) {
      return false;
    }
    Map<Type, Type> next = new HashMap<>(bindings);
    TypeVariableSupport.bind((Class<?>) rawType, parameterized.getActualTypeArguments(), next);
    for (Type interfaceType : ((Class<?>) rawType).getGenericInterfaces()) {
      if (resolvesToByteBuffer(interfaceType, next)) {
        return true;
      }
    }
    Type superclass = ((Class<?>) rawType).getGenericSuperclass();
    return resolvesToByteBuffer(superclass, next);
  }

  private static Type resolve(Type type, Map<Type, Type> bindings) {
    Type resolved = type;
    while (resolved instanceof java.lang.reflect.TypeVariable<?> && bindings.containsKey(resolved)) {
      Type next = bindings.get(resolved);
      if (next == resolved) {
        break;
      }
      resolved = next;
    }
    return resolved;
  }

  private static final class TypeVariableSupport {
    private TypeVariableSupport() {}

    static void bind(Class<?> rawType, Type[] arguments, Map<Type, Type> bindings) {
      java.lang.reflect.TypeVariable<?>[] variables = rawType.getTypeParameters();
      for (int i = 0; i < variables.length && i < arguments.length; i++) {
        bindings.put(variables[i], arguments[i]);
      }
    }
  }
}
