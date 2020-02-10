package com.intorqa.verticle;

import com.intorqa.verticle.filter.DeduplicationFilter;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public class FileSystemWatcher extends AbstractVerticle {
  private static final Logger logger = LoggerFactory.getLogger(FileSystemWatcher.class);
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  private static volatile boolean isStarted;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    isStarted = true;
    String dirName = config().getString("observable-directory");
    Path path = new File(dirName).toPath();

    logger.info(String.format("Watching directory %s", path.toAbsolutePath()));
    boolean directory = Files.isDirectory(path);
    if (!directory) {
      startPromise.fail(String.format("Provided path does not exist %s", path));
    }

    watchDirectory(path);

    startPromise.complete();
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    isStarted = false;
    executorService.shutdown();

    stopPromise.complete();
  }

  public void watchDirectory(Path path) throws IOException {
    EventBus eventBus = vertx.eventBus();
    executorService.submit(() -> {
      FileSystem fs = path.getFileSystem();

      try (WatchService service = fs.newWatchService()) {
        path.register(service, ENTRY_CREATE, ENTRY_MODIFY);
        WatchKey key;

        while (isStarted) {
          key = service.take();
          for (WatchEvent<?> watchEvent : key.pollEvents()) {
            WatchEvent.Kind<?> kind = watchEvent.kind();
            if (ENTRY_CREATE == kind) {
              Path newPath = ((WatchEvent<Path>) watchEvent).context();
              logger.info("New item created: " + newPath);
              String aPath = new File(path.toAbsolutePath().toString(), newPath.toString()).getAbsolutePath();
              sendUpdate(aPath, eventBus, kind);
            } else if (ENTRY_MODIFY == kind) {
              Path newPath = ((WatchEvent<Path>) watchEvent).context();
              logger.info("New path modified: " + newPath);
              String aPath = new File(path.toAbsolutePath().toString(), newPath.toString()).getAbsolutePath();
              sendUpdate(aPath, eventBus, kind);
            }
          }

          if (!key.reset()) {
            break;
          }
        }

      } catch (Exception e) {
        logger.error(String.format("Failed to watch the directory %s", path), e);
      }

    });

  }

  private void sendUpdate(String path, EventBus eventBus, WatchEvent.Kind<?> kind) {
    JsonObject payload = new JsonObject()
      .put("path", path)
      .put("event", kind.toString())
      .put("nanoTimestamp", System.nanoTime());
    eventBus.send(DeduplicationFilter.FILE_SYSTEM_WATCHER_DEDUPLICATION_FILTER, payload);
  }
}
