package com.pixipanda.kafkatraining.serialization.customSerialization;

import java.util.Date;

/**
 * Created by kafka on 10/1/19.
 */
public class Vendor {
    private int vendorId;
    private String vendorName;
    private Date vendorRegistrationDate;

    public Vendor(int vendorId, String vendorName, Date vendorRegistrationDate) {
        this.vendorId = vendorId;
        this.vendorName = vendorName;
        this.vendorRegistrationDate = vendorRegistrationDate;
    }

    public int getVendorId() {
        return vendorId;
    }

    public void setVendorId(int vendorId) {
        this.vendorId = vendorId;
    }

    public String getVendorName() {
        return vendorName;
    }

    public void setVendorName(String vendorName) {
        this.vendorName = vendorName;
    }

    public Date getVendorRegistrationDate() {
        return vendorRegistrationDate;
    }

    public void setVendorRegistrationDate(Date vendorRegistrationDate) {
        this.vendorRegistrationDate = vendorRegistrationDate;
    }

    @Override
    public String toString() {
        return "Vendor{" +
                "vendorId=" + vendorId +
                ", vendorName='" + vendorName + '\'' +
                ", vendorRegistrationDate=" + vendorRegistrationDate +
                '}';
    }

}
