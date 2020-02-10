package com.intorqa.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class WordsAnalytics extends AbstractVerticle {
  private static final Logger logger = LoggerFactory.getLogger(WordsAnalytics.class);
  public static final String FILESYSTEM_ANALYZE_TEXT_TO_ANALYZE = "filesystem.analyze.text.to.analyze";
  public static final String FILESYSTEM_ANALYZE_FILE_PARSED = "filesystem.analyze.file.parsed";
  public static final String FILESYSTEM_ANALYZE_REQUEST_STATISTICS = "filesystem.analyze.file.requestStatistics";

  //store in memory just for demo purposes
  private static volatile AtomicLong parsedFilesCount = new AtomicLong(0);
  private static volatile AtomicLong parsedWordsCount = new AtomicLong(0);

  private static volatile Set<String> uniqueWords = new ConcurrentHashSet<>();
  private static final Map<String, AtomicLong> wordsStatistics = new ConcurrentHashMap<>();

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    EventBus eventBus = vertx.eventBus();
    eventBus.consumer(FILESYSTEM_ANALYZE_TEXT_TO_ANALYZE, this::analyze);
    eventBus.consumer(FILESYSTEM_ANALYZE_FILE_PARSED, this::fileParsed);
    eventBus.consumer(FILESYSTEM_ANALYZE_REQUEST_STATISTICS, this::requestStatistics);
    startPromise.complete();
  }

  private void requestStatistics(Message<JsonObject> tMessage) {
    JsonObject response = new JsonObject();
    response.put("parsedFilesCount", parsedFilesCount.longValue());
    response.put("parsedWordsCount", parsedWordsCount.longValue());
    response.put("uniqueWordsCount", uniqueWords.size());
    Map map = getTop10();
    response.put("top10repeating", new JsonObject(map));
    tMessage.reply(response);
  }

  private Map<String, AtomicLong> getTop10() {
    Map<String, AtomicLong> sorted = wordsStatistics.entrySet()
      .stream()
      .sorted((o1, o2) -> (int) (o2.getValue().longValue() - o1.getValue().longValue()))
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        Map.Entry::getValue,
        (oldValue, newValue) -> oldValue, LinkedHashMap::new));

    return sorted.entrySet()
      .stream()
      .limit(10)
      .collect(LinkedHashMap::new, (k, v) -> k.put(v.getKey(), v.getValue()), Map::putAll);
  }


  private void fileParsed(Message<JsonObject> tMessage) {
    parsedFilesCount.incrementAndGet();//The total number of files processed so far
  }

  private void analyze(Message<JsonObject> tMessage) {
    List<String> words = tMessage.body().getJsonArray("words").getList();
    parsedWordsCount.addAndGet(words.size());//The total number of words processed so far
    uniqueWords.addAll(words);//The total number of unique words processed so far.

    vertx.<Void>executeBlocking(future -> {
      words.forEach(word -> {
        wordsStatistics.compute(word, (k, v) -> {
          if (Objects.isNull(v)) {
            return new AtomicLong(1);
          } else {
            v.incrementAndGet();
            return v;
          }
        });
      });
      future.complete();
    }, res -> {
      if (res.succeeded()) {
        String msg = String.format("Analyzes completed for %d words for file %s", words.size(), tMessage.body().getString("path"));
        logger.info(msg);
      } else {
        String msg = String.format("Analyzes failed for %d words for file %s", words.size(), tMessage.body().getString("path"));
        logger.error(msg);
      }
    });
  }

}
