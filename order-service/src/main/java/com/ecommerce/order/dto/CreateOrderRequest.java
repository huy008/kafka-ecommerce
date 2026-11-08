package com.ecommerce.order.dto;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

public class CreateOrderRequest {

    @NotBlank(message = "Mã khách hàng (customerId) không được để trống")
    private String customerId;

    @NotBlank(message = "Tên sản phẩm (productName) không được để trống")
    private String productName;

    @Min(value = 1, message = "Số lượng (quantity) phải ít nhất là 1")
    private Integer quantity = 1;

    private Double unitPrice;

    @NotNull(message = "Số tiền (amount) không được để trống")
    @Positive(message = "Số tiền (amount) phải lớn hơn 0")
    private Double amount;

    private String paymentMethod = "CREDIT_CARD";
    private String shippingAddress = "Hà Nội, Việt Nam";

    public CreateOrderRequest() {}

    public CreateOrderRequest(String customerId, String productName, Double amount) {
        this.customerId = customerId;
        this.productName = productName;
        this.amount = amount;
        this.unitPrice = amount;
        this.quantity = 1;
    }

    public CreateOrderRequest(String customerId, String productName, Integer quantity, Double unitPrice,
                              Double amount, String paymentMethod, String shippingAddress) {
        this.customerId = customerId;
        this.productName = productName;
        this.quantity = quantity != null ? quantity : 1;
        this.unitPrice = unitPrice;
        this.amount = amount;
        this.paymentMethod = paymentMethod != null ? paymentMethod : "CREDIT_CARD";
        this.shippingAddress = shippingAddress != null ? shippingAddress : "Hà Nội, Việt Nam";
    }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getProductName() { return productName; }
    public void setProductName(String productName) { this.productName = productName; }

    public Integer getQuantity() { return quantity; }
    public void setQuantity(Integer quantity) { this.quantity = quantity; }

    public Double getUnitPrice() { return unitPrice; }
    public void setUnitPrice(Double unitPrice) { this.unitPrice = unitPrice; }

    public Double getAmount() { return amount; }
    public void setAmount(Double amount) { this.amount = amount; }

    public String getPaymentMethod() { return paymentMethod; }
    public void setPaymentMethod(String paymentMethod) { this.paymentMethod = paymentMethod; }

    public String getShippingAddress() { return shippingAddress; }
    public void setShippingAddress(String shippingAddress) { this.shippingAddress = shippingAddress; }
}

