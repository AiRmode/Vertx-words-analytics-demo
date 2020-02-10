package com.intorqa.verticle.filter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import static com.intorqa.verticle.processmanager.FileReaderProcessManager.FILEREADER_PROCESSMANAGER_FILE_NEW;

public class DeduplicationFilter extends AbstractVerticle {
  public static final String FILE_SYSTEM_WATCHER_DEDUPLICATION_FILTER = "filesystem.watcher.updates";

  @Override
  public void start() {
    EventBus eventBus = vertx.eventBus();
    eventBus.consumer(FILE_SYSTEM_WATCHER_DEDUPLICATION_FILTER, this::update);
  }

  private void update(Message<JsonObject> tMessage) {
    //additional steps in order to avoid parsing the same file more then once to be here
    //...
    EventBus eventBus = vertx.eventBus();
    eventBus.send(FILEREADER_PROCESSMANAGER_FILE_NEW, tMessage.body());
  }
}
