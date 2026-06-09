# Changelog Stream Color Coding

This document explains the color scheme used in the ChangelogFormatter for Flink Table API changelog streams.

## Color Scheme Overview

The color scheme follows Flink's changelog semantics and uses intuitive colors to represent different operations:

```
🟢 Green:  +I (Insert operations)
🔴 Red:    -D (Delete operations)
🟡 Yellow: -U (Update before)
🟠 Orange: +U (Update after)
🔵 Blue:   Query names/prefixes
🟣 Purple: Field names
```

## Detailed Color Mapping

### Operation Types

| Operation | Symbol | Color | Visual | Meaning |
|-----------|--------|-------|--------|---------|
| INSERT | +I | 🟢 Bold Green | `[INSERT]` | New record added to the result |
| UPDATE_BEFORE | -U | 🟡 Yellow | `[UPDATE-]` | Old version before update |
| UPDATE_AFTER | +U | 🟠 Bold Orange | `[UPDATE+]` | New version after update |
| DELETE | -D | 🔴 Bold Red | `[DELETE]` | Record removed from result |

### Data Elements

| Element | Color | Purpose |
|---------|-------|---------|
| Query Name | 🔵 Bold Blue | Identifies which query produced the result |
| Field Names | 🟣 Magenta | Distinguishes field names from values |
| String Values | Green | Text/string data |
| Numeric Values | Cyan | Numbers (integers, floats, doubles) |
| Timestamps | Gray | Millisecond timestamps (when enabled) |
| Null Values | Gray | Missing/null data |

## Example Output

Here's what you'll see in your terminal:

```
[1762952249603] Customer Spending > [INSERT]    customerId='customer_001', total_spent=450.50, order_count=3
[1762952249604] Customer Spending > [UPDATE-]  customerId='customer_001', total_spent=450.50, order_count=3
[1762952249605] Customer Spending > [UPDATE+]  customerId='customer_001', total_spent=650.75, order_count=4
[1762952249606] Customer Spending > [DELETE]    customerId='customer_001', total_spent=650.75, order_count=4
```

With actual colors:
- Gray timestamp `[1762952249603]`
- Bold blue query name `Customer Spending`
- Green bold `[INSERT]` or Orange bold `[UPDATE+]`
- Magenta field names `customerId`, `total_spent`, `order_count`
- Green strings `'customer_001'`
- Cyan numbers `450.50`, `3`

## Why These Colors?

### Traffic Light Metaphor
- 🟢 **Green (INSERT)**: Go! New data flowing in
- 🟡 **Yellow (UPDATE_BEFORE)**: Caution! Data changing
- 🟠 **Orange (UPDATE_AFTER)**: Proceed! New value applied
- 🔴 **Red (DELETE)**: Stop! Data removed

### Semantic Meaning
- **Green**: Positive action (new data)
- **Yellow**: Warning (old value being replaced)
- **Orange**: Transition (new value after change)
- **Red**: Alert (data removal)

### Visual Hierarchy
- **Bold colors** (Green, Orange, Red) for operations that change state
- **Regular yellow** for transient old values
- **Blue** for context (query name)
- **Magenta** for structure (field names)
- **Cyan/Green** for data values
- **Gray** for metadata (timestamps, nulls)

## Understanding Changelog Streams

When you see output like this:

```
[UPDATE-]  customerId='customer_001', total_spent=450.50, order_count=3
[UPDATE+]  customerId='customer_001', total_spent=650.75, order_count=4
```

It means:
1. 🟡 The old aggregated value was `total_spent=450.50, order_count=3`
2. 🟠 The new aggregated value is `total_spent=650.75, order_count=4`
3. A new order of `200.25` was added for customer_001

## Disabling Colors

If you need plain text output (for logs or non-color terminals):

```java
// Without colors
resultStream
    .map(ChangelogFormatter.detailedNoColor("Query Name"))
    .print();
```

## Testing Colors

Run the color demo to see all colors in action:

```bash
./gradlew runColorDemo
```

This will display examples of all operation types with their respective colors.

## Terminal Compatibility

Colors work in:
- ✅ macOS Terminal
- ✅ iTerm2
- ✅ Linux terminals (bash, zsh, fish)
- ✅ Windows Terminal
- ✅ VS Code integrated terminal
- ✅ IntelliJ IDEA terminal
- ✅ Most modern terminal emulators

Colors may not work in:
- ❌ Very old terminal emulators
- ❌ Some CI/CD log viewers
- ❌ Text editors without ANSI support

For these cases, use the `*NoColor` formatter variants.
