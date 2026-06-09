# ChangelogFormatter Utility

A utility class for formatting Flink Table API changelog stream output with ANSI colors for better readability.

## Features

- **Color-coded operations**: Different colors for INSERT, UPDATE, and DELETE operations
- **Field highlighting**: Field names and values are color-coded by type
- **Timestamp support**: Optional millisecond timestamps for each record
- **Customizable**: Can disable colors for environments that don't support ANSI codes

## Color Scheme

Following Flink's changelog semantics:

| Element | Color | Emoji | Description |
|---------|-------|-------|-------------|
| INSERT (+I) | 🟢 Bold Green | 🟢 | New records added to the stream |
| UPDATE_BEFORE (-U) | 🟡 Yellow | 🟡 | Old version of a record before update |
| UPDATE_AFTER (+U) | 🟠 Bold Orange | 🟠 | New version of a record after update |
| DELETE (-D) | 🔴 Bold Red | 🔴 | Records removed from the stream |
| Query Prefix | 🔵 Bold Blue | 🔵 | The name of the query (e.g., "Customer Spending") |
| Field Names | 🟣 Magenta | 🟣 | Names of fields in the record |
| String Values | Green | | String field values (quoted) |
| Numeric Values | Cyan | | Integer and floating-point values |
| Timestamps | Gray | | Millisecond timestamps in brackets |
| Null Values | Gray | | Null field values |

## Usage Examples

### Basic Usage (with colors and timestamps)

```java
DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
resultStream
    .map(ChangelogFormatter.detailed("Customer Spending"))
    .print();
```

### Simple Format (no timestamps, with colors)

```java
resultStream
    .map(ChangelogFormatter.simple("Category Performance"))
    .print();
```

### Without Colors (for log files)

```java
resultStream
    .map(ChangelogFormatter.detailedNoColor("Hourly Sales"))
    .print();
```

### Custom Configuration

```java
resultStream
    .map(ChangelogFormatter.colored("My Query", true))  // with timestamp
    .print();
```

## Example Output

With colors enabled, you'll see output like:

```
[1762952249603] Customer Spending > [INSERT]    customerId='customer_001', total_spent=450.50, order_count=3
[1762952249604] Customer Spending > [UPDATE-]  customerId='customer_001', total_spent=450.50, order_count=3
[1762952249605] Customer Spending > [UPDATE+]  customerId='customer_001', total_spent=650.75, order_count=4
```

Where:
- 🟢 Green bold INSERT (+I)
- 🟡 Yellow UPDATE_BEFORE (-U)
- 🟠 Orange bold UPDATE_AFTER (+U)
- 🔴 Red bold DELETE (-D)
- 🔵 Blue bold query name
- 🟣 Magenta field names
- Green strings, Cyan numbers
- Gray timestamps

## Testing

Run the color demo to see all colors in action:

```bash
./gradlew runColorDemo
```

## Implementation Details

The formatter implements `MapFunction<Row, String>` and can be used in any DataStream transformation pipeline. It uses ANSI escape codes for terminal colors, which are supported by most modern terminals including:

- macOS Terminal
- iTerm2
- Linux terminals (bash, zsh, etc.)
- Windows Terminal
- VS Code integrated terminal
- IntelliJ IDEA terminal

## Factory Methods

| Method | Timestamps | Colors | Use Case |
|--------|-----------|--------|----------|
| `detailed(prefix)` | ✓ | ✓ | Default - full information with colors |
| `simple(prefix)` | ✗ | ✓ | Cleaner output without timestamps |
| `detailedNoColor(prefix)` | ✓ | ✗ | Log files or non-color terminals |
| `simpleNoColor(prefix)` | ✗ | ✗ | Minimal output for logs |
| `colored(prefix, showTimestamp)` | Custom | ✓ | Full control over settings |

## Notes

- Colors are automatically disabled if the output is redirected to a file
- The formatter is thread-safe and can be used in parallel streams
- Numeric values are formatted with 2 decimal places for consistency
- Field names are extracted from the Row schema when available
