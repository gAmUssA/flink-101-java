package com.example.flink.lesson05;

/**
 * Simple demo to show the ANSI color codes used in ChangelogFormatter
 * Run this to see what the colored output looks like
 */
public class ColorDemo {
    
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
    
    public static void main(String[] args) {
        System.out.println("\n=== ChangelogFormatter Color Demo ===\n");
        
        // Show INSERT operation
        System.out.println(GRAY + "[14:23:45.123]" + RESET + " " +
                          BOLD + BLUE + "Customer Spending" + RESET + " > " +
                          GREEN + BOLD + "[INSERT]   " + RESET + " " +
                          MAGENTA + "customerId" + RESET + "=" + GREEN + "'customer_001'" + RESET + ", " +
                          MAGENTA + "total_spent" + RESET + "=" + CYAN + "450.50" + RESET + ", " +
                          MAGENTA + "order_count" + RESET + "=" + CYAN + "3" + RESET);
        
        // Show UPDATE_BEFORE operation
        System.out.println(GRAY + "[14:23:45.456]" + RESET + " " +
                          BOLD + BLUE + "Customer Spending" + RESET + " > " +
                          YELLOW + "[UPDATE-] " + RESET + " " +
                          MAGENTA + "customerId" + RESET + "=" + GREEN + "'customer_001'" + RESET + ", " +
                          MAGENTA + "total_spent" + RESET + "=" + CYAN + "450.50" + RESET + ", " +
                          MAGENTA + "order_count" + RESET + "=" + CYAN + "3" + RESET);
        
        // Show UPDATE_AFTER operation
        System.out.println(GRAY + "[14:23:45.789]" + RESET + " " +
                          BOLD + BLUE + "Customer Spending" + RESET + " > " +
                          ORANGE + BOLD + "[UPDATE+] " + RESET + " " +
                          MAGENTA + "customerId" + RESET + "=" + GREEN + "'customer_001'" + RESET + ", " +
                          MAGENTA + "total_spent" + RESET + "=" + CYAN + "650.75" + RESET + ", " +
                          MAGENTA + "order_count" + RESET + "=" + CYAN + "4" + RESET);
        
        // Show DELETE operation
        System.out.println(GRAY + "[14:23:46.012]" + RESET + " " +
                          BOLD + BLUE + "Customer Spending" + RESET + " > " +
                          RED + BOLD + "[DELETE]   " + RESET + " " +
                          MAGENTA + "customerId" + RESET + "=" + GREEN + "'customer_001'" + RESET + ", " +
                          MAGENTA + "total_spent" + RESET + "=" + CYAN + "650.75" + RESET + ", " +
                          MAGENTA + "order_count" + RESET + "=" + CYAN + "4" + RESET);
        
        System.out.println("\n=== Timestamp Format Examples ===\n");
        
        // Show different timestamp formats
        System.out.println("TIME_ONLY:  " + GRAY + "[14:23:45.123]" + RESET + " " + 
                          BOLD + BLUE + "Query" + RESET + " > " + GREEN + BOLD + "[INSERT]" + RESET);
        System.out.println("DATETIME:   " + GRAY + "[2024-11-12 14:23:45.123]" + RESET + " " + 
                          BOLD + BLUE + "Query" + RESET + " > " + GREEN + BOLD + "[INSERT]" + RESET);
        System.out.println("RELATIVE:   " + GRAY + "[+0.123s]" + RESET + " " + 
                          BOLD + BLUE + "Query" + RESET + " > " + GREEN + BOLD + "[INSERT]" + RESET);
        System.out.println("MILLIS:     " + GRAY + "[1762952249603]" + RESET + " " + 
                          BOLD + BLUE + "Query" + RESET + " > " + GREEN + BOLD + "[INSERT]" + RESET);
        
        System.out.println("\n=== Color Legend ===");
        System.out.println("🟢 " + GREEN + BOLD + "Green" + RESET + "    - +I (INSERT) operations & string values");
        System.out.println("🟡 " + YELLOW + "Yellow" + RESET + "   - -U (UPDATE_BEFORE) operations");
        System.out.println("🟠 " + ORANGE + BOLD + "Orange" + RESET + "   - +U (UPDATE_AFTER) operations");
        System.out.println("🔴 " + RED + BOLD + "Red" + RESET + "      - -D (DELETE) operations");
        System.out.println("🔵 " + BOLD + BLUE + "Blue" + RESET + "     - Query prefix/name");
        System.out.println("🟣 " + MAGENTA + "Magenta" + RESET + "  - Field names");
        System.out.println("   " + CYAN + "Cyan" + RESET + "     - Numeric values");
        System.out.println("   " + GRAY + "Gray" + RESET + "     - Timestamps & null values");
        System.out.println();
    }
}
