package com.intorqa.verticle;

import com.intorqa.verticle.processmanager.FileReaderProcessManager;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class FileReader extends AbstractVerticle {
  private static final Logger logger = LoggerFactory.getLogger(FileReader.class);
  public static final String FILE_READER = "filesystem.files.read";
  public static final String FILEREAD_STATUS_READING_DONE = "done";
  public static final String FILEREAD_STATUS_READING_INPROGRESS = "reading";

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    EventBus eventBus = vertx.eventBus();
    eventBus.consumer(FILE_READER, this::update);
  }

  private <T> void update(Message<T> tMessage) {
    JsonObject body = (JsonObject) tMessage.body();
    logger.info("New event received: " + body);

    String path = body.getString("path");
    long nanoTimestamp = body.getLong("nanoTimestamp");

    readFile(path, nanoTimestamp);
  }

  private void readFile(String path, long nanoTimestamp) {
    EventBus eventBus = vertx.eventBus();
    OpenOptions opts = new OpenOptions().setRead(true);
    vertx.fileSystem().open(path, opts, ar -> {
      if (ar.succeeded()) {
        AsyncFile file = ar.result();
        file.handler(buffer -> {
          String chunk = buffer.toString(StandardCharsets.UTF_8);
          eventBus.send(FileReaderProcessManager.FILEREADER_PROCESSMANAGER_FILE_READING_INPROGRESS, new JsonObject()
            .put("nanoTimestamp", nanoTimestamp)
            .put("path", path)
            .put("chunk", chunk)
            .put("status", FILEREAD_STATUS_READING_INPROGRESS));
        })
          .exceptionHandler(err -> logger.error("Error during file reading", err))
          .endHandler(done -> {
            eventBus.send(FileReaderProcessManager.FILEREADER_PROCESSMANAGER_FILE_READING_DONE, new JsonObject()
              .put("nanoTimestamp", nanoTimestamp)
              .put("path", path)
              .put("status", FILEREAD_STATUS_READING_DONE));
          });
      } else {
        logger.error("Can not open file to read " + path, ar.cause());
      }
    });
  }


}
