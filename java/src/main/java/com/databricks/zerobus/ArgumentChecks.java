package com.databricks.zerobus;

import java.util.Objects;

final class ArgumentChecks {

  private ArgumentChecks() {}

  static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.trim().isEmpty()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
