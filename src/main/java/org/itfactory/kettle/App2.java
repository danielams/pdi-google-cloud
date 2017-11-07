/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.itfactory.kettle;

import com.google.cloud.bigquery.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Second test app for bulk loading data from google cloud storage to google bigquery
 */
public class App2 {
    public static void main(String[] args) {
        String datasetName = "test";
        String tableName = "rwa";
        String sourceUri = "gs://cloud-test-123/rwa/out/*";
        String delimiter = ";";

        int leadingRowsToSkip = 1;
        boolean createDataset = true;
        boolean createTable = true;

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        DatasetInfo datasetInfo = Dataset.of(datasetName);
        Dataset dataset = bigquery.getDataset(datasetInfo.getDatasetId());

        if(dataset == null && createDataset) {
            bigquery.create(datasetInfo);
        } else {
            System.out.println("Please create the dataset: " + datasetName);
            System.exit(1);
        }

        TableId tableId = TableId.of(dataset.getDatasetId().getDataset(), tableName);
        Table table = bigquery.getTable(tableId);

        if (table == null && createTable) {
            List<Field> fieldList = new ArrayList<Field>();
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

            Schema schema = Schema.of(fieldList);

            table = bigquery.create(TableInfo.of(tableId, StandardTableDefinition.of(schema)));
        } else {
            System.out.println("Please create the table: " + tableName);
            System.exit(1);
        }

        Job loadJob = table.load(CsvOptions.newBuilder().setFieldDelimiter(delimiter).setSkipLeadingRows(leadingRowsToSkip).build(), sourceUri);

        try {
            loadJob = loadJob.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (loadJob.getStatus().getError() != null) {
            System.out.println("Job completed with errors");
            System.out.println(loadJob.getStatus().getError().getMessage());
        } else {
            System.out.println("Job succeeded");
        }
    }
}
