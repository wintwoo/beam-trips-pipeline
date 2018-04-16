#!/bin/bash
mvn clean compile && \
mvn exec:java -Dexec.mainClass="com.example.TripPipeline" \
  -e -Dexec.args="--project=wwoo-gcp --tempLocation='gs://wwoo-gcp/temp' --streaming=true --filePattern='gs://wwoo-gcp/trips/*.csv' --flightAffinitiesTable='wwoo-gcp:trips.flight_affinities' --lifetimePointsTable='wwoo-gcp:trips.passenger_points' --runner=DirectRunner"
