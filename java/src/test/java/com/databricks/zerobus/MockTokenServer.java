package com.databricks.zerobus;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Loopback Unity Catalog token endpoint for federation tests. It records every request and replies
 * from a script, then with a 200 carrying a distinct access token ({@code dbx-token-<n>}).
 */
final class MockTokenServer implements AutoCloseable {

  /** One scripted reply. */
  static final class Reply {
    final int status;
    final int expiresIn;
    final String body;

    private Reply(int status, int expiresIn, String body) {
      this.status = status;
      this.expiresIn = expiresIn;
      this.body = body;
    }

    static Reply ok(int expiresIn) {
      return new Reply(200, expiresIn, null);
    }

    static Reply error(int status, String body) {
      return new Reply(status, 0, body);
    }
  }

  /** One request the endpoint received. */
  static final class Request {
    final String method;
    final String path;
    final Map<String, List<String>> headers;
    final Map<String, List<String>> form;

    Request(
        String method,
        String path,
        Map<String, List<String>> headers,
        Map<String, List<String>> form) {
      this.method = method;
      this.path = path;
      this.headers = headers;
      this.form = form;
    }

    String header(String name) {
      for (Map.Entry<String, List<String>> e : headers.entrySet()) {
        if (e.getKey().equalsIgnoreCase(name) && !e.getValue().isEmpty()) {
          return e.getValue().get(0);
        }
      }
      return null;
    }

    String formValue(String name) {
      List<String> values = form.get(name);
      return values == null || values.isEmpty() ? null : values.get(0);
    }
  }

  private final HttpServer server;
  private final List<Request> requests = Collections.synchronizedList(new ArrayList<>());
  private final Deque<Reply> script = new ArrayDeque<>();

  MockTokenServer(Reply... replies) throws IOException {
    Collections.addAll(script, replies);
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handle);
    server.start();
  }

  String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  List<Request> requests() {
    synchronized (requests) {
      return new ArrayList<>(requests);
    }
  }

  int count() {
    return requests.size();
  }

  private void handle(HttpExchange exchange) throws IOException {
    String body = new String(readAll(exchange.getRequestBody()), StandardCharsets.UTF_8);
    Reply reply;
    int n;
    synchronized (requests) {
      n = requests.size();
      requests.add(
          new Request(
              exchange.getRequestMethod(),
              exchange.getRequestURI().getPath(),
              new HashMap<>(exchange.getRequestHeaders()),
              parseForm(body)));
      synchronized (script) {
        reply = script.isEmpty() ? Reply.ok(3600) : script.poll();
      }
    }
    String response =
        reply.status == 200
            ? "{\"access_token\":\"dbx-token-"
                + n
                + "\",\"token_type\":\"Bearer\",\"expires_in\":"
                + reply.expiresIn
                + "}"
            : reply.body;
    byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(reply.status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private static Map<String, List<String>> parseForm(String body) throws IOException {
    Map<String, List<String>> form = new HashMap<>();
    if (body.isEmpty()) {
      return form;
    }
    for (String pair : body.split("&")) {
      int eq = pair.indexOf('=');
      String key = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), "UTF-8");
      String value = eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), "UTF-8");
      form.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
    return form;
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    byte[] chunk = new byte[8192];
    int r;
    while ((r = in.read(chunk)) != -1) {
      buf.write(chunk, 0, r);
    }
    return buf.toByteArray();
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
