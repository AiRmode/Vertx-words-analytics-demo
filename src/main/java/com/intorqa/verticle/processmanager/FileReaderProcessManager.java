package com.intorqa.verticles.processmanager;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.intorqa.verticles.FileReader.FILE_READER;
import static com.intorqa.verticles.processmanager.FileParserProcessManager.FILEPARSER_PROCESSMANAGER_FILE_READING_NEW;

public class FileReaderProcessManager extends AbstractVerticle {
  private static final Logger logger = LoggerFactory.getLogger(FileReaderProcessManager.class);

  public static final String FILEREADER_PROCESSMANAGER_FILE_NEW = "filesystem.processManager.file.reading.new";
  public static final String FILEREADER_PROCESSMANAGER_FILE_READING_INPROGRESS = "filesystem.processManager.file.reading.inprogress";
  public static final String FILEREADER_PROCESSMANAGER_FILE_READING_DONE = "filesystem.processManager.file.reading.done";

  @Override
  public void start(Promise<Void> startPromise) {
    EventBus eventBus = vertx.eventBus();
    //read files from filesystem
    eventBus.consumer(FILEREADER_PROCESSMANAGER_FILE_NEW, this::handleNewFile);
    eventBus.consumer(FILEREADER_PROCESSMANAGER_FILE_READING_INPROGRESS, this::handleFileReadingInProgress);
    eventBus.consumer(FILEREADER_PROCESSMANAGER_FILE_READING_DONE, this::handleFileReadingDone);

    startPromise.complete();
  }

  private void handleFileReadingDone(Message<JsonObject> tMessage) {
    JsonObject body = tMessage.body();
    logger.info("Reading for the file completed: " + body.getString("path"));
  }

  private void handleFileReadingInProgress(Message<JsonObject> tMessage) {
    logger.info("Reading file in progress:" + tMessage.body().getString("path"));
    EventBus eventBus = vertx.eventBus();
    eventBus.send(FILEPARSER_PROCESSMANAGER_FILE_READING_NEW, tMessage.body());
  }

  private void handleNewFile(Message<JsonObject> tMessage) {
    logger.info("Starting to read the file: " + tMessage.body().getString("path"));
    EventBus eventBus = vertx.eventBus();
    eventBus.send(FILE_READER, tMessage.body());
  }

}
