package com.intorqa.domain.object;

import io.vertx.core.json.JsonObject;

import java.util.List;

public class FileAnalytics extends JsonObject {
  private String path;
  private long nanoTimestamp;
  private List<String> words;

  public FileAnalytics(String path, long nanoTimestamp, List<String> words) {
    this.words = words;
    this.path = path;
    this.nanoTimestamp = nanoTimestamp;
  }

  public String getPath() {
    return path;
  }

  public long getNanoTimestamp() {
    return nanoTimestamp;
  }

  public List<String> getWords() {
    return words;
  }

}
