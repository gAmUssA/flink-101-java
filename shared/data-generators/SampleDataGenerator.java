package shared.data.generators;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Sample Data Generator for Educational Flink Lessons
 * <p>
 * This utility provides various datasets for experimentation across different lessons.
 * Designed for educational clarity - simple, understandable data that demonstrates concepts clearly.
 * <p>
 * Usage Examples:
 * - Use different datasets to see how word counts change
 * - Experiment with varying data sizes and complexity
 * - Test edge cases like empty strings, special characters, etc.
 */
public class SampleDataGenerator {

  private static final Random random = new Random();

  /**
   * Basic word count dataset - simple sentences about Flink and stream processing
   * Perfect for understanding basic DataStream transformations
   */
  public static List<String> getBasicWordCountData() {
    return Arrays.asList(
        "apache flink streaming data processing",
        "real time analytics with apache flink",
        "flink datastream api tutorial",
        "streaming word count example",
        "apache flink lesson one"
    );
  }

  /**
   * Extended dataset with more variety - good for testing window operations
   * Contains repeated words and phrases to demonstrate aggregation
   */
  public static List<String> getExtendedWordCountData() {
    return Arrays.asList(
        "apache flink is a powerful stream processing framework",
        "stream processing enables real time data analytics",
        "flink provides both datastream and table api",
        "apache flink supports exactly once processing semantics",
        "real time stream processing with apache flink",
        "flink datastream api offers flexible transformations",
        "stream processing framework for big data analytics",
        "apache flink tutorial for beginners",
        "flink streaming jobs process data in real time",
        "datastream transformations in apache flink"
    );
  }

  /**
   * E-commerce themed dataset - useful for business logic examples
   * Demonstrates realistic use cases for stream processing
   */
  public static List<String> getEcommerceData() {
    return Arrays.asList(
        "customer placed order for laptop computer",
        "payment processed for mobile phone purchase",
        "order shipped laptop to customer address",
        "customer reviewed laptop product positively",
        "inventory updated after mobile phone sale",
        "customer support ticket for laptop issue",
        "promotional email sent for computer deals",
        "customer browsed mobile phone categories",
        "order cancelled for computer purchase",
        "customer loyalty points earned from purchase"
    );
  }

  /**
   * IoT sensor themed dataset - good for time-series processing examples
   * Shows how stream processing applies to sensor data
   */
  public static List<String> getIoTSensorData() {
    return Arrays.asList(
        "temperature sensor reading twenty five degrees",
        "humidity sensor detected high moisture levels",
        "motion sensor triggered in living room",
        "temperature dropped to twenty degrees outside",
        "pressure sensor reading normal atmospheric levels",
        "humidity sensor reading low moisture content",
        "motion sensor activated in kitchen area",
        "temperature sensor malfunction detected immediately",
        "pressure sensor calibration completed successfully",
        "humidity levels increased after rain event"
    );
  }

  /**
   * Mixed complexity dataset - contains various edge cases
   * Good for testing tokenization and filtering logic
   */
  public static List<String> getMixedComplexityData() {
    return Arrays.asList(
        "Simple sentence with basic words",
        "Numbers like 123 and 456 mixed with text",
        "Special characters: @#$% and punctuation!",
        "UPPERCASE and lowercase and MiXeD cAsE",
        "",  // Empty string to test edge cases
        "   whitespace   around   words   ",
        "Repeated repeated repeated words for testing",
        "Single",
        "Very long sentence with many words to test window aggregation and processing capabilities",
        "unicode-characters: café naïve résumé"
    );
  }

  /**
   * Generate random sentences for stress testing
   * Useful for performance testing and large dataset simulation
   */
  public static List<String> generateRandomSentences(int count) {
    String[] words = {
        "apache", "flink", "streaming", "data", "processing", "real", "time",
        "analytics", "framework", "java", "scala", "api", "datastream", "table",
        "window", "aggregation", "transformation", "source", "sink", "operator",
        "parallelism", "checkpoint", "state", "watermark", "event", "batch"
    };

    List<String> sentences = new java.util.ArrayList<>();

    for (int i = 0; i < count; i++) {
      StringBuilder sentence = new StringBuilder();
      int wordsInSentence = 3 + random.nextInt(8); // 3-10 words per sentence

      for (int j = 0; j < wordsInSentence; j++) {
          if (j > 0) {
              sentence.append(" ");
          }
        sentence.append(words[random.nextInt(words.length)]);
      }

      sentences.add(sentence.toString());
    }

    return sentences;
  }

  /**
   * Get dataset by name - convenient method for lesson examples
   * Makes it easy to switch between different datasets in lessons
   */
  public static List<String> getDataset(String datasetName) {
    switch (datasetName.toLowerCase()) {
      case "basic":
        return getBasicWordCountData();
      case "extended":
        return getExtendedWordCountData();
      case "ecommerce":
        return getEcommerceData();
      case "iot":
        return getIoTSensorData();
      case "mixed":
        return getMixedComplexityData();
      case "random":
        return generateRandomSentences(20);
      default:
        System.out.println("Unknown dataset: " + datasetName + ". Using basic dataset.");
        return getBasicWordCountData();
    }
  }

  /**
   * Print available datasets - helpful for learners to discover options
   */
  public static void printAvailableDatasets() {
    System.out.println("Available datasets:");
    System.out.println("- basic: Simple Flink-related sentences");
    System.out.println("- extended: More variety with repeated words");
    System.out.println("- ecommerce: Business-themed data");
    System.out.println("- iot: IoT sensor data simulation");
    System.out.println("- mixed: Various edge cases and complexity");
    System.out.println("- random: Randomly generated sentences");
    System.out.println("\nUsage: SampleDataGenerator.getDataset(\"datasetName\")");
  }
}