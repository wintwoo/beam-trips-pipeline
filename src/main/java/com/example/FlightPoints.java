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
public class FlightPoints {

  String customerNo;
  Integer points;
  String date;

  public FlightPoints() {
  }

  public FlightPoints(String customerNo, Integer points, String date) {
    this.customerNo = customerNo;
    this.points = points;
    this.date = date;
  }

  public String getCustomerNo() {
    return this.customerNo;
  }

  public Integer getPoints() {
    return this.points;
  }

  public String getDate() {
    return this.date;
  }

  public void setCustomerNo(String customerNo) {
    this.customerNo = customerNo;
  }

  public void setPoints(Integer points) {
    this.points = points;
  }

  public void setDate(String date) {
    this.date = date;
  }
}
