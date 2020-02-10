package com.intorqa.verticles;

import com.intorqa.domain.objects.FileAnalytics;
import com.intorqa.verticles.processmanager.FileParserProcessManager;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.intorqa.verticles.processmanager.FileParserProcessManager.FILEPARSER_PROCESSMANAGER_FILE_READING_DONE;
import static com.intorqa.verticles.processmanager.FileParserProcessManager.FILEPARSER_PROCESSMANAGER_FILE_READING_FAILED;

public class FileParser extends AbstractVerticle {
  private static final Logger logger = LoggerFactory.getLogger(FileParserProcessManager.class);
  public static final String FILE_PARSER_PARSE_FILE = "filesystem.files.parse";

  private final Pattern wordsRegexp = Pattern.compile("\\w+");

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    EventBus eventBus = vertx.eventBus();
    eventBus.consumer(FILE_PARSER_PARSE_FILE, this::parseFile);

    startPromise.complete();
  }

  private void parseFile(Message<JsonObject> tMessage) {
    JsonObject body = tMessage.body();
    String chunk = body.getString("chunk");
    String path = body.getString("path");
    long nanoTimestamp = body.getLong("nanoTimestamp");

    handleNewFile(chunk, path, nanoTimestamp);
  }

  private void handleNewFile(String chunk, String path, long nanoTimestamp) {
    EventBus eventBus = vertx.eventBus();
    vertx.<FileAnalytics>executeBlocking(future -> {
      try {
        FileAnalytics analytics = getWords(chunk, path, nanoTimestamp);
        future.complete(analytics);
      } catch (Exception e) {
        logger.error("Failed to parse chunk " + new JsonObject()
          .put("path", path)
          .put("nanoTimestamp", nanoTimestamp)
          .put("exception", e));
        future.fail(e);
      }
    }, res -> {
      if (res.succeeded()) {
        FileAnalytics analytics = res.result();
        eventBus.send(FILEPARSER_PROCESSMANAGER_FILE_READING_DONE, new JsonObject()
          .put("path", analytics.getPath())
          .put("nanoTimestamp", analytics.getNanoTimestamp())
          .put("words", Arrays.toString(analytics.getWords())));
      } else {
        eventBus.send(FILEPARSER_PROCESSMANAGER_FILE_READING_FAILED, new JsonObject()
          .put("path", path)
          .put("nanoTimestamp", nanoTimestamp)
          .put("exception", res.cause()));
      }
    });

  }

  private FileAnalytics getWords(String chunk, String path, long nanoTimestamp) throws Exception {
    List<String> words = new LinkedList<>();
    Matcher matcher = wordsRegexp.matcher(chunk);
    while (matcher.find()) {
      String group = matcher.group();
      words.add(group);
    }

    return new FileAnalytics(path, nanoTimestamp, words.toArray(new String[0]));
  }
}
