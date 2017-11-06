package org.itfactory.kettle;

import com.google.cloud.bigquery.*;
import com.google.auth.oauth2.GoogleCredentials;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;

import java.io.FileInputStream;
import java.io.IOException;

public class Stream1 {
    public static void main(String[] args) {
        System.out.println("In main of Stream1");
        String datasetName = "streamdata";
        String tableName = "ages";

        int leadingRowsToSkip = 1;
        boolean createDataset = true;
        boolean createTable = true;
        try {

        BigQueryOptions options = BigQueryOptions.newBuilder()
        .setProjectId("bigquery-testing-184814")
        .setCredentials(GoogleCredentials.fromStream(
          new FileInputStream("C:/Users/afowler/Documents/Apps/google-cloud/BigQueryTesting-fd7a2959ea0f.json"))
        ).build();
        BigQuery bigquery = options.getService();

        DatasetInfo datasetInfo = Dataset.of(datasetName);
        Dataset dataset = bigquery.getDataset(datasetInfo.getDatasetId());

        if (dataset == null) {
            if (createDataset) {
                System.out.println("Creating dataset");
                dataset = bigquery.create(datasetInfo);
            } else {
                System.out.println("Please create the dataset: " + datasetName);
                System.exit(1);
            }
        }

        TableId tableId = TableId.of(dataset.getDatasetId().getDataset(), tableName);
        Table table = bigquery.getTable(tableId);

        if (table == null) {
            if (createTable) {
                System.out.println("Creating table");
            List<Field> fieldList = new ArrayList<Field>();
            /*
            fieldList.add(Field.of("product_id", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("facility_id", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("customer_id", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("collateral_id", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("exposure_type", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("collateral_type", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("exposure", LegacySQLTypeName.FLOAT));
            fieldList.add(Field.of("pd", LegacySQLTypeName.FLOAT));
            fieldList.add(Field.of("lgd", LegacySQLTypeName.FLOAT));
            fieldList.add(Field.of("maturity", LegacySQLTypeName.FLOAT));
            */
            // sales_data.csv fields - BigQuery import doesn't like this data for some reason... nulls???
            /*
            fieldList.add(Field.of("ORDERNUMBER", LegacySQLTypeName.INTEGER));
            fieldList.add(Field.of("QUANTITYORDERED", LegacySQLTypeName.INTEGER));
            fieldList.add(Field.of("PRICEEACH", LegacySQLTypeName.FLOAT));
            fieldList.add(Field.of("ORDERLINENUMBER", LegacySQLTypeName.INTEGER));
            fieldList.add(Field.of("SALES", LegacySQLTypeName.FLOAT));
            fieldList.add(Field.of("ORDERDATE", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("STATUS", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("QTR_ID", LegacySQLTypeName.INTEGER));
            fieldList.add(Field.of("MONTH_ID", LegacySQLTypeName.INTEGER));
            fieldList.add(Field.of("YEAR_ID", LegacySQLTypeName.INTEGER));
            fieldList.add(Field.of("PRODUCTLINE", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("MSRP", LegacySQLTypeName.FLOAT));
            fieldList.add(Field.of("PRODUCTCODE", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("CUSTOMERNAME", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("PHONE", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("ADDRESSLINE1", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("ADDRESSLINE2", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("CITY", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("STATE", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("POSTALCODE", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("COUNTRY", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("TERRITORY", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("CONTACTLASTNAME", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("CONTACTFIRSTNAME", LegacySQLTypeName.STRING));
            */
            // sample age data csv
            fieldList.add(Field.of("Name", LegacySQLTypeName.STRING));
            fieldList.add(Field.of("Age", LegacySQLTypeName.INTEGER));


            Schema schema = Schema.of(fieldList);

            table = bigquery.create(TableInfo.of(tableId, StandardTableDefinition.of(schema)));
        } else {
            System.out.println("Please create the table: " + tableName);
            System.exit(1);
        }
    } // end table null if

// stream results here
System.out.println("Writing rows to BigQuery");

//TableId tableId = TableId.of(datasetName, tableName);
// Values of the row to insert
Map<String, Object> rowContent1 = new HashMap<String,Object>();
rowContent1.put("Name", "AF");
rowContent1.put("Age", 36);
Map<String, Object> rowContent2 = new HashMap<String,Object>();
rowContent2.put("Name", "WF");
rowContent2.put("Age", 30);
// Records are passed as a map
InsertAllRequest insertBuilder = InsertAllRequest.newBuilder(tableId)
.addRow("34", rowContent2)
.addRow("35", rowContent1)
    // More rows can be added in the same RPC by invoking .addRow() on the builder
    .build();
InsertAllResponse response = bigquery.insertAll(insertBuilder);
if (response.hasErrors()) {
  // If any of the insertions failed, this lets you inspect the errors
  for (Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
    // inspect row error
    System.out.println(entry.toString());
  }
}
System.out.println("Stream1 completed");


} catch (IOException ioe) {
    ioe.printStackTrace(System.out);
}
System.out.println("End of Stream1");
  } // end main function
} // end class