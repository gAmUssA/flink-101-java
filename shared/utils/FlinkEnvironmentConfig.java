package utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Utility class for configuring Flink execution environments
 * <p>
 * This class provides methods to create Flink environments that can connect
 * to the Flink Web UI for monitoring and debugging. It supports both:
 * - Local execution with Web UI enabled
 * - Remote cluster execution (Docker)
 * <p>
 * Educational Benefits:
 * - Visualize your streaming pipelines in the Flink Web UI
 * - Monitor job execution, metrics, and performance
 * - Debug issues using the execution graph
 * - Understand operator chaining and parallelism
 */
public class FlinkEnvironmentConfig {

    /**
     * Creates a StreamExecutionEnvironment with Web UI enabled
     * <p>
     * This allows you to access the Flink Web UI at http://localhost:8081
     * to monitor your jobs, view execution graphs, and check metrics.
     * <p>
     * The environment is configured for educational clarity:
     * - Parallelism set to 1 for easier observation
     * - Operator chaining disabled to see individual steps
     * - Checkpointing enabled for fault tolerance
     * 
     * @return Configured StreamExecutionEnvironment
     */
    public static StreamExecutionEnvironment createEnvironmentWithUI() {
        Configuration config = new Configuration();
        
        // Enable REST API for Web UI
        config.set(RestOptions.BIND_PORT, "8081");
        config.set(RestOptions.ENABLE_FLAMEGRAPH, true);
        
        // Create environment with configuration
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        
        // Configure for educational clarity
        env.setParallelism(1);
        env.disableOperatorChaining();
        
        // Enable checkpointing for fault tolerance (60 seconds)
        env.enableCheckpointing(60000);
        
        return env;
    }

    /**
     * Creates a standard local StreamExecutionEnvironment
     * <p>
     * This is the simple version without Web UI, useful for quick testing
     * or when you don't need visualization.
     * 
     * @return Configured StreamExecutionEnvironment
     */
    public static StreamExecutionEnvironment createLocalEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure for educational clarity
        env.setParallelism(1);
        env.disableOperatorChaining();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(60000);
        
        return env;
    }

    /**
     * Prints instructions for accessing the Flink Web UI
     */
    public static void printWebUIInstructions() {
        System.out.println("╔════════════════════════════════════════════════════════════╗");
        System.out.println("║           Flink Web UI Access Instructions                ║");
        System.out.println("╠════════════════════════════════════════════════════════════╣");
        System.out.println("║  Open your browser and navigate to:                       ║");
        System.out.println("║  → http://localhost:8081                                   ║");
        System.out.println("║                                                            ║");
        System.out.println("║  In the Web UI you can:                                    ║");
        System.out.println("║  • View the execution graph of your streaming pipeline     ║");
        System.out.println("║  • Monitor job status and metrics in real-time             ║");
        System.out.println("║  • Check task manager resources and performance            ║");
        System.out.println("║  • View checkpoints and savepoints                         ║");
        System.out.println("║  • Debug issues using logs and flame graphs                ║");
        System.out.println("╚════════════════════════════════════════════════════════════╝");
        System.out.println();
    }
}
