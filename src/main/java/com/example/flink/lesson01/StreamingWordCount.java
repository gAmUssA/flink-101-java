package com.example.flink.lesson01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import shared.data.generators.SampleDataGenerator;
import utils.FlinkEnvironmentConfig;

/**
 * Lesson 1: DataStream API with In-Memory Data
 * <p>
 * This lesson demonstrates the fundamentals of Apache Flink's DataStream API using simple,
 * in-memory data sources. Perfect for understanding core concepts without external dependencies.
 * <p>
 * What you'll learn:
 * - How to set up a StreamExecutionEnvironment
 * - Creating data streams from in-memory collections
 * - Applying basic transformations (flatMap, keyBy, sum)
 * - Understanding parallelism and operator chaining
 * - Observing stream processing in the Flink Web UI
 * <p>
 * Expected Output:
 * You should see incremental word counts printed to the console as each sentence is processed:
 * (apache,1) -> (apache,2) -> (apache,3)
 * (flink,1) -> (flink,2) -> (flink,3) -> (flink,4)
 * (streaming,1) -> (streaming,2)
 * (data,1)
 * (processing,1)
 * <p>
 * Try this:
 * 1. Experiment with different datasets: change "basic" to "extended", "ecommerce", "iot", "mixed", or "random"
 * 2. Use SampleDataGenerator.printAvailableDatasets() to see all options
 * 3. Compare word counts between different datasets to understand data variety
 * 4. Modify the parallelism settings and observe processing behavior
 * 5. Add more transformation steps to the pipeline (filtering, mapping, etc.)
 */
public class StreamingWordCount {

  public static void main(String[] args) throws Exception {

    // Step 1: Create the execution environment with Web UI enabled
    // This allows you to visualize your pipeline at http://localhost:8081
    StreamExecutionEnvironment env = FlinkEnvironmentConfig.createEnvironmentWithUI();

    System.out.println("=== Flink Lesson 1: Streaming Word Count ===");
    System.out.println("Starting stream processing with in-memory data...");
    System.out.println();
    
    // Print Web UI access instructions
    FlinkEnvironmentConfig.printWebUIInstructions();

    // Show available datasets for experimentation
    SampleDataGenerator.printAvailableDatasets();
    System.out.println();

    // Step 2: Create a data stream from in-memory data using SampleDataGenerator
    // The SampleDataGenerator provides various datasets for experimentation:
    // - "basic": Simple Flink-related sentences (default for this lesson)
    // - "extended": More variety with repeated words for window operations
    // - "ecommerce": Business-themed data for realistic examples
    // - "iot": IoT sensor data for time-series processing
    // - "mixed": Various edge cases and complexity levels
    // - "random": Randomly generated sentences for stress testing

    // Try changing "basic" to other dataset names to see different results!

//    DataStreamSource<String> textLines = env.fromData(
//        "apache flink streaming data processing",
//        "real time analytics with apache flink",
//        "flink datastream api tutorial",
//        "streaming word count example",
//        "apache flink lesson one"
//    );
    DataStreamSource<String> textLines = env.fromData(
        SampleDataGenerator.getDataset("basic")
    );

    // Step 3: Transform the data using the DataStream API
    // This creates a processing pipeline that will:
    // 1. Split each line into words (flatMap with Tokenizer)
    // 2. Group by word (keyBy)
    // 3. Count occurrences (sum) - aggregates across all data
    DataStream<Tuple2<String, Integer>> wordCounts = textLines
        .flatMap(new Tokenizer())                                    // Split lines into words
        .keyBy(value -> value.f0)            // Group by word (first field)
        .sum(1);                                     // Sum the counts (second field)

    // Step 4: Output the results
    // In production, you might write to Kafka, databases, or files
    wordCounts.print("Word Count Results");

    // Step 5: Execute the program
    // Nothing happens until you call execute() - this triggers the actual processing
    env.execute("Lesson 1: Streaming Word Count with In-Memory Data");

    System.out.println("Stream processing completed!");
  }

  /**
   * Tokenizer: A simple function that splits text lines into individual words
   * <p>
   * This implements FlatMapFunction, which means:
   * - Input: One element (a line of text)
   * - Output: Zero or more elements (individual words as Tuple2<word, count>)
   * <p>
   * Try this:
   * - Modify the regex pattern to handle different word separators
   * - Add filtering for common stop words ("the", "and", "or", etc.)
   * - Convert to lowercase for case-insensitive counting
   * - Add minimum word length filtering
   */
  public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {

      // Convert to lowercase for consistent counting
      String cleanLine = line.toLowerCase().trim();

      // Split the line into words using whitespace and punctuation as separators
      // The regex \\W+ matches one or more non-word characters
      String[] words = cleanLine.split("\\W+");

      // Emit each word with an initial count of 1
      for (String word : words) {
        // Only emit non-empty words (filter out empty strings from splitting)
        if (!word.isEmpty()) {
          // Create a tuple: (word, 1)
          // The "1" will be summed up later in the aggregation step
          out.collect(new Tuple2<>(word, 1));

          // Educational debugging: uncomment to see each word being processed
          // System.out.println("Processing word: " + word);
        }
      }
    }
  }
}