package com.intorqa.main;

import com.intorqa.verticle.*;
import com.intorqa.verticle.filter.DeduplicationFilter;
import com.intorqa.verticle.processmanager.FileParserProcessManager;
import com.intorqa.verticle.processmanager.FileReaderProcessManager;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class AppMain {

  public static final String CLI_ARG_CONF = "conf";

  public static void main(String[] args) throws ParseException {
    JsonObject properties = getConfig(args);
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new FileSystemWatcher(), new DeploymentOptions().setConfig(properties));
    vertx.deployVerticle(new DeduplicationFilter(), new DeploymentOptions().setConfig(properties));
    vertx.deployVerticle(new FileReaderProcessManager(), new DeploymentOptions().setConfig(properties));
    vertx.deployVerticle(new FileReader(), new DeploymentOptions().setConfig(properties));
    vertx.deployVerticle(new FileParserProcessManager(), new DeploymentOptions().setConfig(properties));
    vertx.deployVerticle(new FileParser(), new DeploymentOptions().setConfig(properties));
    vertx.deployVerticle(new WordsAnalytics(), new DeploymentOptions().setConfig(properties));
    vertx.deployVerticle(new WebServer(), new DeploymentOptions().setConfig(properties));
  }

  private static JsonObject getConfig(String[] args) throws ParseException {
    Options options = new Options();
    options.addOption(CLI_ARG_CONF, true, "path to config file");
    CommandLineParser parser = new DefaultParser();
    CommandLine parse = parser.parse(options, args);
    if (parse.hasOption(CLI_ARG_CONF)) {
      String conf = parse.getOptionValue(CLI_ARG_CONF);
      return fromFile(conf);
    }
    return null;
  }

  public static JsonObject fromFile(String file){
    try {
      return new JsonObject(new String(Files.readAllBytes(Paths.get(file))));
    } catch (IOException e) {
      throw new RuntimeException("Could not read file " + file, e);
    }
  }
}
