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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.Counter;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.coders.Coder.Context;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TripPipeline {

  static TableSchema getSchema(String tablename) {

    List <TableFieldSchema> fields = new ArrayList<>();
    if (tablename.equals("flightAffinities")) {
      fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
      fields.add(new TableFieldSchema().setName("eticketno").setType("STRING"));
      fields.add(new TableFieldSchema().setName("customerno").setType("STRING"));
      fields.add(new TableFieldSchema().setName("flight1").setType("STRING"));
      fields.add(new TableFieldSchema().setName("flight2").setType("STRING"));
    }
    else if (tablename.equals("lifetimePoints")) {
      fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
      fields.add(new TableFieldSchema().setName("customerno").setType("STRING"));
      fields.add(new TableFieldSchema().setName("totalpoints").setType("INTEGER"));
    }

    TableSchema schema = new TableSchema().setFields(fields);
    return schema;
  }

  static class ParseTripRecordDoFn extends DoFn<String, TripRecord> {

    // Log and count parse errors.
    private static final Logger LOG = LoggerFactory.getLogger(ParseTripRecordDoFn.class);
    private final Counter numParseErrors = Metrics.counter("main", "ParseErrors");

    @ProcessElement
    public void processElement(ProcessContext c) {
      String value = c.element();
      if (value.startsWith("#")) {
        return;
      }

      String[] components = value.split(",");
      try {
        Long loadId = Long.parseLong(components[0].trim());
        String customerNo = components[1].trim();
        String eticketNo = components[2].trim();
        String flightCode = components[3].trim();
        Integer points = Integer.parseInt(components[4].trim());
        String date = components[5].trim();
        TripRecord tripRecord = new TripRecord(customerNo, eticketNo, flightCode, points, date, loadId);

        // Output element
        c.output(tripRecord);
      }
      catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
        LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
      }
    }
  }


  static class CreateFlightPairingsDoFn extends DoFn<KV<String, Iterable<TripRecord>>, KV<Long, FlightPairing>> {

    private static final Logger LOG = LoggerFactory.getLogger(CreateFlightPairingsDoFn.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
      Iterable<TripRecord> iterable = c.element().getValue();
      Iterator<TripRecord> itr = iterable.iterator();

      List<TripRecord> flights = new ArrayList<TripRecord>();
      long loadId = 0;

      // Convert iterable to ArrayList
      while (itr.hasNext()) {
        TripRecord tripRecord = itr.next();
        flights.add(tripRecord);

        // Use the largest loadId as the BQ table suffix
        if (tripRecord.getLoadId() > loadId) {
          loadId = tripRecord.getLoadId();
        }
      }

      // Output a row for each flight pairing
      for (int i = 0; i < flights.size(); i++) {
        for (int j = 0; j < flights.size(); j++) {

          FlightPairing pairing = new FlightPairing(
              flights.get(i).getCustomerNo(),
              flights.get(i).getEticketNo(),
              flights.get(i).getFlightCode(),
              flights.get(j).getFlightCode(),
              flights.get(j).getDate()); // we're just going to use the date for flight2 in the pairing

          // Debug
          LOG.info("Pairings: " + flights.get(i).getCustomerNo() + " " + flights.get(i).getFlightCode()
            + " " + flights.get(j).getFlightCode() + " Pane: " + c.pane().getIndex());

          // Output element
          c.output(KV.of(loadId, pairing));
        }
      }
    }
  }


  public static class CalculateLifetimePoints
    extends PTransform<PCollection<KV<String, TripRecord>>, PCollection<FlightPoints>> {

    private static final Logger LOG = LoggerFactory.getLogger(CreateFlightPairingsDoFn.class);

    static class ExtractPointsDoFn extends DoFn<KV<String, TripRecord>, KV<String, Integer>> {
      @ProcessElement
      public void processElement(ProcessContext c) {
        TripRecord tripRecord = c.element().getValue();
        c.output(KV.of(c.element().getKey(), tripRecord.getPoints()));
      }
    }


    @Override
    public PCollection<FlightPoints> expand(PCollection<KV<String, TripRecord>> kvs) {
      return kvs
        .apply("ExtractPoints", ParDo.of(new ExtractPointsDoFn()))
        .apply("CustomerTotalPoints", Sum.<String>integersPerKey())
        .apply("CustomerPointsToTableRow", MapElements.via(
          new SimpleFunction<KV<String, Integer>, FlightPoints>() {
            public FlightPoints apply(KV<String, Integer> v) {
              LOG.info("PointsToTable: " + v.getKey() + " " + v.getValue());
              return new FlightPoints(v.getKey(), v.getValue(), Instant.now().toString());
            }
          }
        ));
    }
  }


  public static void main(String[] args) {

    // Create a PipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the runner you wish to use. This example
    // will run with the DirectRunner by default, based on the class path configured
    // in its dependencies.

    TripPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(TripPipelineOptions.class);;

    Pipeline p = Pipeline.create(options);

    // Read lines from incoming files into windowed input
    PCollection<TripRecord> newRecords = p
      .apply("ReadMatchingFilesFromGCS", TextIO.read()
        .from(options.getFilePattern())
        // Check for new files every 10 seconds
        .watchForNewFiles(Duration.standardSeconds(5),
        // Never stop checking for new files
        Watch.Growth.<String>never()))
      // Parse each line into a TripRecord emitted with timestamp
      .apply("ParseRecords", ParDo.of(new ParseTripRecordDoFn()))

      //
      // Apply this transform instead windowing by timestamp in the data
      // withAllowedTimeStampSkew is deprecated
      //
      // .apply("AddTimestamps", WithTimestamps.<TripRecord>of(x -> Instant.parse(x.getDate()))
      //    .withAllowedTimestampSkew(Duration.millis(Long.MAX_VALUE)))

      .apply("AddTimestamps", WithTimestamps.<TripRecord>of(x -> Instant.now()))
      // Apply 10 year fixed windows
      .apply(Window.<TripRecord>into(FixedWindows.of(Duration.standardDays(365*10)))
                                     // Repeatedly trigger after first element in the pane
                                     .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                                        // With speculative results every 30 seconds
                                        .plusDelayOf(Duration.standardSeconds(5))))
                                     // No allowed lateness
                                     .withAllowedLateness(Duration.ZERO)
                                     // Accumulating all previously fired panes
                                     .accumulatingFiredPanes());

    // Customer lifetime points
    PCollection<FlightPoints> lifetimePoints = newRecords
      // Extract customerNo from TripRecord and use as group key
      .apply("EmitWithCustomerNoAsKey", WithKeys.of(new SerializableFunction<TripRecord, String>() {
        @Override
        public String apply(TripRecord tripRecord) {
          return (String)tripRecord.getCustomerNo();
        }
      }))
      .apply("CalculateLifetimePoints", new CalculateLifetimePoints());


    // Customer per-trip flightCode affinities
    PCollection<KV<Long, FlightPairing>> flightPairings = newRecords
      // Extract customerNo from TripRecord and use as group key
      .apply("EmitWithEticketNoAsKey", WithKeys.of(new SerializableFunction<TripRecord, String>() {
        @Override
        public String apply(TripRecord tripRecord) {
          return (String)tripRecord.getEticketNo();
        }
      }))
      .apply("GroupRecordsByEticketNo", GroupByKey.<String, TripRecord>create())
      .apply("ProcessCustomerFlightAffinities", ParDo.of(new CreateFlightPairingsDoFn()));

    // Write flight affinities to BigQuery
    flightPairings
      .apply("WriteFlightAffinitiesToBQ", BigQueryIO.<KV<Long, FlightPairing>>write()
        .withSchema(getSchema("flightAffinities"))
        .withFormatFunction(new SerializableFunction<KV<Long, FlightPairing>, TableRow>() {
          public TableRow apply(KV<Long, FlightPairing> kv) {
            FlightPairing pairing = kv.getValue();
            DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            return new TableRow()
              .set("customerno", pairing.getCustomerNo())
              .set("eticketno", pairing.getEticketNo())
              .set("flight1", pairing.getFlight1())
              .set("flight2", pairing.getFlight2())
              .set("timestamp", Instant.parse(pairing.getDate(), dtf).toString());
          }
        })
        .to((ValueInSingleWindow<KV<Long, FlightPairing>> v) -> {
          return new TableDestination(
            "wwoo-gcp:trips.all_flights_" + v.getValue().getKey(),
            "Flight affinities for pane number " + v.getValue().getKey()
          );
        })
        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    // Write per-customer lifetime points to BigQuery
    lifetimePoints
      .apply("WriteLifetimePointsToBQ", BigQueryIO.<FlightPoints>write()
        .to(options.getLifetimePointsTable())
        .withSchema(getSchema("lifetimePoints"))
        .withFormatFunction(new SerializableFunction<FlightPoints, TableRow>() {
          public TableRow apply(FlightPoints points) {
            return new TableRow()
              .set("customerno", points.getCustomerNo())
              .set("totalpoints", points.getPoints())
              .set("timestamp", Instant.parse(points.getDate()).toString());
          }
        })
        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
        .withSchema(getSchema("lifetimePoints"))
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    // Run pipeline
    p.run().waitUntilFinish();
  }
}
