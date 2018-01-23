# pdi-bigquery

This repository holds Open Source components for integrating Pentaho Data Integration (PDI) with Google's Cloud platform.

## Contents

The following custom steps are contained within the google-cloud plugin:-

- Google BigQuery Loader Job Entry - Loads one or more CSV, JSON, or Avro files from Google Cloud Storage in to Google BigQuery
- Google BigQuery Stream Step - Loads a PDI Stream's rows in to Google BigQuery using the Streaming API

## Building the code

You can use Microsoft Visual Studio Code (free) to build this, or manually.

### Visual Studio Code Build

1. Ensure you have Oracle Java 8 SDK (not JRE) installed on your computer
1. Ensure you have installed Maven globally on your computer
1. Open the pdi-bigquery folder in Microsoft Code
1. Ensure you have the 'Language support for Java(tm) by RedHat' extension installed in Microsoft Visual Studio Code
1. Go to the Tasks menu, and choose Run Build Task...
1. Run the 'verify' build task

This will download all dependencies, and then build the project.

Note: On second and subsequent builds there is no need to download the dependencies again. To do this, run the verify-offline task instead.

### Manual Build

Change to the root directory (the same as this readme file), and type the following:-

```sh
mvn -B test -e
```

Note: After the first successful build you can tell Maven to not re-fetch build dependencies by using the o flag:-

```sh
mvn -B test -e -o
```

## Installing the plugin

You now have a ZIP file in assemblies/plugin/target/ unpack this in to your PENTAHO_HOME/design-tools/data-integration/plugins folder.

## Samples

Sample age data in a variety of formats (JSON, CSV and (uncompressed) Avro) are provided for Age data. This dataset has two fields - Name:string and Age:int. Files are located in plugin/assembly/resources/samples

A sample Job for the BigQuery bulk load from Google Cloud Storage, an a sample Transformation to load using Streaming in to BigQuery, are available in the above folder's transformations subfolder.

## Errata / Bug reporting

Please submit new Issues on this GitHub repository to log any problems or feature suggestions you may have

## License and Copyright

All material in this repository are Copyright 2002-2018 Hitachi Vantara. All code is licensed as Apache 2.0 unless explicitly stated. See the LICENSE file for more details.

## Support Statement

This work is at Stage 1 : Development Phase: Start-up phase of an internal project. Usually a Labs experiment. (Unsupported)