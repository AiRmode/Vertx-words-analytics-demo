package com.intorqa.verticles.processmanager;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.intorqa.verticles.FileParser.FILE_PARSER_PARSE_FILE;

public class FileParserProcessManager extends AbstractVerticle {
  private static final Logger logger = LoggerFactory.getLogger(FileParserProcessManager.class);

  public static final String FILEPARSER_PROCESSMANAGER_FILE_READING_NEW = "filesystem.processManager.file.parser.new";
  public static final String FILEPARSER_PROCESSMANAGER_FILE_READING_DONE = "filesystem.processManager.file.parser.done";
  public static final String FILEPARSER_PROCESSMANAGER_FILE_READING_FAILED = "filesystem.processManager.file.parser.failed";

  @Override
  public void start(Promise<Void> startPromise) {
    EventBus eventBus = vertx.eventBus();
    eventBus.consumer(FILEPARSER_PROCESSMANAGER_FILE_READING_NEW, this::handleNewFile);
    eventBus.consumer(FILEPARSER_PROCESSMANAGER_FILE_READING_DONE, this::handleFileParsingDone);
    eventBus.consumer(FILEPARSER_PROCESSMANAGER_FILE_READING_FAILED, this::handleFileParsingFailed);

    startPromise.complete();
  }

  private void handleFileParsingFailed(Message<JsonObject> tMessage) {
    logger.error("failed to parse the message " + tMessage);
  }

  private void handleNewFile(Message<JsonObject> tMessage) {
    EventBus eventBus = vertx.eventBus();
    eventBus.send(FILE_PARSER_PARSE_FILE, tMessage.body());
  }

  private void handleFileParsingDone(Message<JsonObject> tMessage) {
    String words = tMessage.body().getString("words");
    logger.info("Parsing done for the file " + tMessage.body().getString("path") + " words: " + words);
  }

}
