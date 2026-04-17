# Past Architecture — Original Java Pipeline

## Context

The marketing industry consolidates data from many different sources — ad platforms, analytics tools, CRM systems, and more. In the original pipeline, three of these sources were brought together: marketing performance data, web analytics (Adobe Analytics and Google Analytics 4), and CRM lead data.

The goal was to produce a single consolidated table that analysts could connect directly to a dashboard. Data flowed from raw ingestion through a silver quality layer into a final gold output in BigQuery.

This architecture was designed and built several years ago — a first hands-on project with Apache Beam, written in Java.

---

## Raw ingestion layer

Each source had its own ingestion process, all converging on a common format before the main pipeline consumed them.

**Adobe Analytics and Google Analytics 4**
Dedicated ingestion processes were built to pull data from each API and write the results to Google Cloud Storage in Avro format. Avro was chosen for its compact binary encoding, schema enforcement, and native compatibility with Dataflow and BigQuery.

**CRM (CSV)**
The client's CRM exported a full weekly snapshot as a CSV file. A Cloud Run service was deployed with a GCS trigger: when the analyst uploaded the weekly CSV to a bucket, the trigger fired the Cloud Run job, which read the file, applied initial data quality checks, wrote the output to Avro in GCS — enabling partitioning and making the data consistent with the other sources — and then automatically kicked off the Dataflow job. No manual pipeline execution was needed after the file upload.

> **Note on the current project:** the decision to keep the raw CSV as-is and read the analytics data in different formats (JSON for GA4, Parquet for Adobe) is intentional. The goal here is to practice reading heterogeneous file formats with Apache Beam, rather than normalising everything to Avro upfront as was done in the original.

---

## Silver layer

Before producing the final output, records that failed taxonomy validation were written to a silver layer in GCS. These were records where field values did not conform to the expected taxonomy — wrong category codes, unrecognised status values, malformed identifiers, and similar issues.

This layer served as an audit trail: it allowed investigation of data quality problems at the source level without losing any records or blocking the main pipeline. The gold layer was then built only from records that passed validation.

---

## Gold layer

The main Beam pipeline read from all three Avro sources, applied the business logic, and produced a single consolidated table in BigQuery.

The key transformations applied at this layer:

- **Consolidation** — all records from the three sources were merged into a unified schema
- **Lead classification** — business rules were applied to classify each CRM lead by funnel stage
- **Campaign attribution** — CRM leads were crossed with analytics data to determine which campaign was responsible for each lead, based on attribution rules defined by the business
- **BigQuery sink** — the final enriched records were written to a BigQuery table, which analysts connected directly to their dashboard tool of choice

Records with taxonomy issues were routed to the silver layer rather than the gold table, keeping the final output clean and dashboard-ready.
