package org.folio.metastorage.server;

import org.graalvm.polyglot.Value;

public final class JavaScriptCheck {
  private JavaScriptCheck() {
    throw new UnsupportedOperationException("JavaScriptCheck");
  }

  /**
   * Run JavaScript compiler/interpreter once.
   */
  public static void check() {
    check("x => 1");
  }

  static void check(String script) {
    org.graalvm.polyglot.Context context = org.graalvm.polyglot.Context.create("js");
    Value func = context.eval("js", script);
    Value value = func.execute();
    boolean ok = value.isNumber() && value.asLong() == 1L;
    context.close();
    if (!ok) {
      throw new IllegalStateException("Unexpected result from javascript engine");
    }
  }
}
