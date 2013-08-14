package org.ops.rabbitmq;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.ops.rabbitmq.test.Consumer;
import org.ops.rabbitmq.test.Producer;

/**
 * 
 */
public class App {
  public static void main(String[] args) {

    Options supportedOptions = new Options()
        .addOption("s", "server", true, "RabbitMQ serer")
        .addOption("p", "port", true, "RabbitMQ port")
        .addOption("e", "exchange", true, "Name of RabbitMQ exchnage")
        .addOption("r", "routing_key", true, "RabbitMQ routing key")
        .addOption("q", "queue", true, "Name of queue")
        .addOption("x", "action", true, "Action, enqueue or dequeue")
        .addOption("i", "init_workers", true, "Initial workers")
        .addOption("m", "max_workers", true, "Maximum workers")
        .addOption("d", "durable", true, "Message durable")
        .addOption("n", "number_messages", true, "The Number of messages");

    // parse arguments
    CommandLine cmd = null;

    try {
      CommandLineParser parser = new GnuParser();
      cmd = parser.parse(supportedOptions, args);
      if (!cmd.hasOption("h")) {
        if ("enqueue".equalsIgnoreCase(cmd.getOptionValue("x"))) {
          enqueue_test(cmd);
        } else if ("dequeue".equalsIgnoreCase(cmd.getOptionValue("x"))) {
          dequeue_test(cmd);
        } else {
          show_usage(supportedOptions);
        }
      } else {
        show_usage(supportedOptions);
      }
    } catch (UnrecognizedOptionException e) {
      System.out.println(e.getMessage());
      show_usage(supportedOptions);
    } catch (Throwable e) {
      System.out.println(e.getMessage());
    }
  }

  private static void show_usage(Options options) {
    HelpFormatter help = new HelpFormatter();
    help.printHelp("RabbitMQ Test Producer", options);
  }

  private static void enqueue_test(CommandLine cmd) {

    String server = cmd.getOptionValue("s", "127.0.0.1");
    int port = Integer.parseInt(cmd.getOptionValue("p", "5672"));
    String exchange = cmd.getOptionValue("e");
    String routingKey = cmd.getOptionValue("r");
    String queue = cmd.getOptionValue("q");
    boolean durable = Boolean.parseBoolean(cmd.getOptionValue("d", "true"));

    int initWorkers = Integer.parseInt(cmd.getOptionValue("i", "16"));
    int maxWorkers = Integer.parseInt(cmd.getOptionValue("m", "16"));
    int numOfMessage = Integer.parseInt(cmd.getOptionValue("n", "1"));

    Producer actor = new Producer(numOfMessage, initWorkers, maxWorkers,
        server, port, exchange, routingKey, queue, durable);

    actor.execute();
  }

  private static void dequeue_test(CommandLine cmd) {

    String server = cmd.getOptionValue("s", "127.0.0.1");
    int port = Integer.parseInt(cmd.getOptionValue("p", "5672"));
    String exchange = cmd.getOptionValue("e");
    String routingKey = cmd.getOptionValue("r");
    String queue = cmd.getOptionValue("q");
    boolean durable = Boolean.parseBoolean(cmd.getOptionValue("d", "true"));

    int maxWorkers = Integer.parseInt(cmd.getOptionValue("m", "5"));
    int numOfMessage = Integer.parseInt(cmd.getOptionValue("n", "10"));

    Consumer actor = new Consumer(numOfMessage, maxWorkers, server, port,
        exchange, routingKey, queue, durable);

    actor.execute();
  }
}
