package shared.data.generators;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Order data class representing an e-commerce order
 * Includes JSON annotations for Kafka serialization
 * <p> 
 * This class is used across multiple lessons for order processing demonstrations.
 * Designed for educational clarity with a simple field structure and JSON support.
 */
public class Order {
    @JsonProperty("orderId")
    public String orderId;
    
    @JsonProperty("customerId")
    public String customerId;
    
    @JsonProperty("amount")
    public double amount;
    
    @JsonProperty("timestamp")
    public long timestamp;
    
    @JsonProperty("category")
    public String category;

    // Default constructor required for JSON deserialization
    public Order() {}

    // Constructor for creating new orders
    public Order(String orderId, String customerId, double amount, long timestamp, String category) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.timestamp = timestamp;
        this.category = category;
    }

    @Override
    public String toString() {
        return String.format("Order{id=%s, customer=%s, amount=%.2f, category=%s}", 
                orderId, customerId, amount, category);
    }

    // Getters and setters for JSON serialization (if needed)
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
    
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }
}