package org.apache.rocketmq.streams.examples.pojo;

public class Order {
    private String type;        // drink, food, 
    private Integer price;          // order price
    private String customer;    // customer name

    public Order() {

    }

    public Order(String type, Integer price, String customer) {
        this.type = type;
        this.price = price;
        this.customer = customer;
    }

    public String getType() {
        return type;
    }

    public String getCustomer() {
        return customer;
    }

    public Integer getPrice() {
        return price;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Order{" + "type=" + type + ", price=" + price + ", customer=" + customer + "}";
    }
}
