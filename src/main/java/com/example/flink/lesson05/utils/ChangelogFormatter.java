package com.example.flink.lesson05.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Utility class for formatting changelog stream output with ANSI colors
 * <p>
 * Provides human-readable formatting for Row objects with RowKind flags:
 * <ul>
 * <li>+I (INSERT): New record added (🟢 GREEN)</li>
 * <li>-U (UPDATE_BEFORE): Old version of updated record (🟡 YELLOW)</li>
 * <li>+U (UPDATE_AFTER): New version of updated record (🟠 ORANGE)</li>
 * <li>-D (DELETE): Record removed (🔴 RED)</li>
 * </ul>
 * <p>
 * Color Scheme:
 * <ul>
 * <li>🟢 Green: +I (Insert operations)</li>
 * <li>🔴 Red: -D (Delete operations)</li>
 * <li>🟡 Yellow: -U (Update before)</li>
 * <li>🟠 Orange: +U (Update after)</li>
 * <li>🔵 Blue: Query names/prefixes</li>
 * <li>🟣 Purple/Magenta: Field names</li>
 * </ul>
 */
public record ChangelogFormatter(String prefix, boolean showTimestamp, boolean useColors, TimestampFormat timestampFormat) implements MapFunction<Row, String> {

  // ANSI Color codes
  private static final String RESET = "\u001B[0m";
  private static final String GREEN = "\u001B[32m";
  private static final String YELLOW = "\u001B[33m";
  private static final String ORANGE = "\u001B[38;5;208m";  // Bright orange (256-color)
  private static final String CYAN = "\u001B[36m";
  private static final String RED = "\u001B[31m";
  private static final String BLUE = "\u001B[34m";
  private static final String MAGENTA = "\u001B[35m";
  private static final String GRAY = "\u001B[90m";
  private static final String BOLD = "\u001B[1m";

  // Timestamp formatters
  private static final DateTimeFormatter TIME_FORMATTER =
      DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
  private static final DateTimeFormatter DATETIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

  /**
   * Timestamp format options
   */
  public enum TimestampFormat {
    /**
     * Time only: HH:mm:ss.SSS
     */
    TIME_ONLY,
    /**
     * Full datetime: yyyy-MM-dd HH:mm:ss.SSS
     */
    DATETIME,
    /**
     * Milliseconds since epoch
     */
    MILLIS,
    /**
     * Relative time from first event (e.g., +1.234s)
     */
    RELATIVE
  }

  private static long firstTimestamp = 0;

  public ChangelogFormatter(String prefix) {
    this(prefix, true, true, TimestampFormat.TIME_ONLY);
  }

  public ChangelogFormatter(String prefix, boolean showTimestamp) {
    this(prefix, showTimestamp, true, TimestampFormat.TIME_ONLY);
  }

  public ChangelogFormatter(String prefix, boolean showTimestamp, boolean useColors) {
    this(prefix, showTimestamp, useColors, TimestampFormat.TIME_ONLY);
  }

  @Override
  public String map(Row row) throws Exception {
    StringBuilder sb = new StringBuilder();

    RowKind kind = row.getKind();

    // Add timestamp if enabled
    if (showTimestamp) {
      if (useColors) {
        sb.append(GRAY);
      }
      sb.append("[").append(formatTimestamp()).append("]");
      if (useColors) {
        sb.append(RESET);
      }
      sb.append(" ");
    }

    // Add prefix with color
    if (prefix != null && !prefix.isEmpty()) {
      if (useColors) {
        sb.append(BOLD).append(BLUE);
      }
      sb.append(prefix);
      if (useColors) {
        sb.append(RESET);
      }
      sb.append(" > ");
    }

    // Format RowKind with colors
    String kindStr = formatRowKind(kind);
    sb.append(kindStr).append(" ");

    // Format row fields
    sb.append(formatRowFields(row));

    // Reset color at the end
    if (useColors) {
      sb.append(RESET);
    }

    return sb.toString();
  }

  /**
   * Formats the current timestamp based on the configured format
   */
  private String formatTimestamp() {
    long now = System.currentTimeMillis();

    return switch (timestampFormat) {
      case TIME_ONLY -> TIME_FORMATTER.format(Instant.ofEpochMilli(now));
      case DATETIME -> DATETIME_FORMATTER.format(Instant.ofEpochMilli(now));
      case MILLIS -> String.valueOf(now);
      case RELATIVE -> {
        if (firstTimestamp == 0) {
          firstTimestamp = now;
          yield "+0.000s";
        }
        double seconds = (now - firstTimestamp) / 1000.0;
        yield String.format("+%.3fs", seconds);
      }
    };
  }

  /**
   * Formats the RowKind with descriptive text and colors
   */
  private String formatRowKind(RowKind kind) {
    if (!useColors) {
      return switch (kind) {
        case INSERT -> "[INSERT]   ";
        case UPDATE_BEFORE -> "[UPDATE-] ";
        case UPDATE_AFTER -> "[UPDATE+] ";
        case DELETE -> "[DELETE]   ";
      };
    }

    return switch (kind) {
      case INSERT -> GREEN + BOLD + "[INSERT]   " + RESET;
      case UPDATE_BEFORE -> YELLOW + "[UPDATE-] " + RESET;
      case UPDATE_AFTER -> ORANGE + BOLD + "[UPDATE+] " + RESET;
      case DELETE -> RED + BOLD + "[DELETE]   " + RESET;
    };
  }

  /**
   * Formats row fields as key-value pairs with colors
   */
  private String formatRowFields(Row row) {
    StringBuilder sb = new StringBuilder();

    int fieldCount = row.getArity();
    String[] fieldNames = row.getFieldNames(true).toArray(new String[0]);

    for (int i = 0; i < fieldCount; i++) {
      if (i > 0) {
        sb.append(", ");
      }

      String fieldName = (fieldNames != null && i < fieldNames.length)
                         ? fieldNames[i]
                         : "field_" + i;

      Object value = row.getField(i);

      // Color the field name
      if (useColors) {
        sb.append(MAGENTA);
      }
      sb.append(fieldName);
      if (useColors) {
        sb.append(RESET);
      }

      sb.append("=");

      // Format and color the value
      String formattedValue = formatValue(value);
      sb.append(formattedValue);
    }

    return sb.toString();
  }

  /**
   * Formats individual field values with colors
   */
  private String formatValue(Object value) {
    StringBuilder sb = new StringBuilder();

    if (value == null) {
      if (useColors) {
        sb.append(GRAY);
      }
      sb.append("null");
      if (useColors) {
        sb.append(RESET);
      }
      return sb.toString();
    }

    if (value instanceof Double || value instanceof Float) {
      if (useColors) {
        sb.append(CYAN);
      }
      sb.append(String.format("%.2f", ((Number) value).doubleValue()));
      if (useColors) {
        sb.append(RESET);
      }
      return sb.toString();
    }

    if (value instanceof Number) {
      if (useColors) {
        sb.append(CYAN);
      }
      sb.append(value);
      if (useColors) {
        sb.append(RESET);
      }
      return sb.toString();
    }

    if (value instanceof String) {
      if (useColors) {
        sb.append(GREEN);
      }
      sb.append("'").append(value).append("'");
      if (useColors) {
        sb.append(RESET);
      }
      return sb.toString();
    }

    return value.toString();
  }

  /**
   * Creates a simple formatter that just shows operation and values (with colors)
   */
  public static MapFunction<Row, String> simple(String prefix) {
    return new ChangelogFormatter(prefix, false, true, TimestampFormat.TIME_ONLY);
  }

  /**
   * Creates a detailed formatter with timestamps (with colors)
   * Uses TIME_ONLY format by default (HH:mm:ss.SSS)
   */
  public static MapFunction<Row, String> detailed(String prefix) {
    return new ChangelogFormatter(prefix, true, true, TimestampFormat.TIME_ONLY);
  }

  /**
   * Creates a detailed formatter with full datetime timestamps
   */
  public static MapFunction<Row, String> detailedWithDate(String prefix) {
    return new ChangelogFormatter(prefix, true, true, TimestampFormat.DATETIME);
  }

  /**
   * Creates a detailed formatter with relative timestamps (+0.123s from start)
   */
  public static MapFunction<Row, String> detailedRelative(String prefix) {
    return new ChangelogFormatter(prefix, true, true, TimestampFormat.RELATIVE);
  }

  /**
   * Creates a simple formatter without colors
   */
  public static MapFunction<Row, String> simpleNoColor(String prefix) {
    return new ChangelogFormatter(prefix, false, false, TimestampFormat.TIME_ONLY);
  }

  /**
   * Creates a detailed formatter without colors
   */
  public static MapFunction<Row, String> detailedNoColor(String prefix) {
    return new ChangelogFormatter(prefix, true, false, TimestampFormat.TIME_ONLY);
  }

  /**
   * Creates a colored formatter with custom settings
   */
  public static MapFunction<Row, String> colored(String prefix, boolean showTimestamp) {
    return new ChangelogFormatter(prefix, showTimestamp, true, TimestampFormat.TIME_ONLY);
  }

  /**
   * Creates a formatter with custom timestamp format
   */
  public static MapFunction<Row, String> withTimestampFormat(String prefix, TimestampFormat format) {
    return new ChangelogFormatter(prefix, true, true, format);
  }
}
