package com.intorqa.domain.objects;

import io.vertx.core.json.JsonObject;

public class FileAnalytics extends JsonObject {
  private String path;
  private long nanoTimestamp;
  private String[] words;

  public FileAnalytics(String path, long nanoTimestamp, String[] words) {
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

  public String[] getWords() {
    return words;
  }

}
