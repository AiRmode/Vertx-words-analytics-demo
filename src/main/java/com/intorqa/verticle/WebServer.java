package com.intorqa.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.templ.freemarker.FreeMarkerTemplateEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class WebServer extends AbstractVerticle {
  private static final Logger logger = LoggerFactory.getLogger(WebServer.class);

  private FreeMarkerTemplateEngine templateEngine;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    startHttpServer().setHandler(ar -> {
      if (ar.succeeded()) {
        startPromise.complete();
      } else {
        startPromise.fail(ar.cause());
      }
    });
  }

  private Future<Void> startHttpServer() {
    Promise<Void> promise = Promise.promise();
    HttpServer server = vertx.createHttpServer();

    templateEngine = FreeMarkerTemplateEngine.create(vertx);
    Router router = Router.router(vertx);
    router.get("/").handler(this::indexHandler);

    server.requestHandler(router).listen(8080, ar -> {
      if (ar.succeeded()) {
        logger.info("HTTP server running on port 8080");
        promise.complete();
      } else {
        logger.error("Could not start a HTTP server", ar.cause());
        promise.fail(ar.cause());
      }
    });

    return promise.future();

  }

  private void indexHandler(RoutingContext context) {
    EventBus eventBus = vertx.eventBus();
    eventBus.request(WordsAnalytics.FILESYSTEM_ANALYZE_REQUEST_STATISTICS, new JsonObject(), h -> {
      if (h.succeeded()) {
        JsonObject body = (JsonObject) h.result().body();

        List<String> pages = new ArrayList<>();
        pages.add("Parsed Files Count: " + body.getLong("parsedFilesCount"));
        pages.add("Parsed Words Count: " + body.getLong("parsedWordsCount"));
        pages.add("Unique Words Count: " + body.getLong("uniqueWordsCount"));
        pages.add("Top 10 repeating words:");
        JsonObject top10repeating = body.getJsonObject("top10repeating");
        top10repeating.getMap().forEach((key, value) -> pages.add(String.format("%s : %s", key, value)));
        context.put("title", "Intorqa words analytics");
        context.put("pages", pages);
        templateEngine.render(context.data(), "templates/index.ftl", ar -> {
          if (ar.succeeded()) {
            context.response().putHeader("Content-Type", "text/html");
            context.response().end(ar.result());
          } else {
            context.fail(ar.cause());
          }
        });
      } else {
        context.response().sendFile("general_error.html");
      }
    });

  }
}
