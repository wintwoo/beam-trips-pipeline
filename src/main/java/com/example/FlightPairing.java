/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class FlightPairing {

  String customerNo;
  String eticketNo;
  String flight1;
  String flight2;
  String date;

  public FlightPairing() {
  }

  public FlightPairing(String customerNo, String eticketNo, String flight1, String flight2, String date) {
    this.customerNo = customerNo;
    this.eticketNo = eticketNo;
    this.flight1 = flight1;
    this.flight2 = flight2;
    this.date = date;
  }

  public String getCustomerNo() {
    return this.customerNo;
  }

  public String getEticketNo() {
    return this.eticketNo;
  }

  public String getFlight1() {
    return this.flight1;
  }

  public String getFlight2() {
    return this.flight2;
  }

  public String getDate() {
    return this.date;
  }

  public void setCustomerNo(String customerNo) {
    this.customerNo = customerNo;
  }

  public void setEticketNo(String eticketNo) {
    this.eticketNo = eticketNo;
  }

  public void setFlight1(String flight1) {
    this.flight1 = flight1;
  }

  public void setFlight2(String flight2) {
    this.flight2 = flight2;
  }

  public void setDate(String date) {
    this.date = date;
  }
}
