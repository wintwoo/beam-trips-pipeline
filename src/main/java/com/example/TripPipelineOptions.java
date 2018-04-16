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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface TripPipelineOptions extends DataflowPipelineOptions {

    @Description("Input file path pattern.")
    @Default.String("gs://wwoo-dev/trips/*.csv")
    @Validation.Required
    String getFilePattern();
    void setFilePattern(String value);

    @Description("BigQuery table for flight affinities "
       + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
    @Default.String("wwoo-gcp:trips.flight_affinities")
    @Validation.Required
    String getFlightAffinitiesTable();
    void setFlightAffinitiesTable(String value);

    @Description("BigQuery table for lifetime passenger points.")
    @Default.String("wwoo-gcp:tripes.passenger_points")
    @Validation.Required
    String getLifetimePointsTable();
    void setLifetimePointsTable(String value);
}
