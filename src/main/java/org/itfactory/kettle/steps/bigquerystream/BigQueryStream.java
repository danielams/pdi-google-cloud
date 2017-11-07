
package org.itfactory.kettle.steps.bigquerystream;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;

import com.google.cloud.bigquery.*;
import com.google.auth.oauth2.GoogleCredentials;

import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Iterator;
import java.util.Set;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * BigQuery Stream Output Step
 *
 * @author afowler
 * @since 06-nov-2017
 */
public class BigQueryStream extends BaseStep implements StepInterface {
  private static Class<?> PKG = BigQueryStreamMeta.class; // for i18n purposes, needed by Translator2!!

  private BigQueryStreamMeta meta;
  private BigQueryStreamData data;

  public BigQueryStream( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (BigQueryStreamMeta) smi;
    data = (BigQueryStreamData) sdi;

    Object[] r = getRow(); // get row, set busy!


    if ( r == null ) {
      // no more input to be expected...
      // finish current stream
      checkBatch(null);
      // clean up connection and environment
      data.insertBuilder = null;
      data.tableId = null;

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
/*
      for ( int i = 0; i < meta.getFieldName().length; i++ ) {
        data.fieldnrs[i] = getInputRowMeta().indexOfValue( meta.getFieldName()[i] );
        if ( data.fieldnrs[i] < 0 ) {
          logError( BaseMessages.getString( PKG, "AggregateRows.Log.CouldNotFindField", meta.getFieldName()[i] ) );
          setErrors( 1 );
          stopAll();
          return false;
        }
      }*/

      // TODO Create connection
      
      try {

      BigQueryOptions options;
      if (meta.getUseContainerSecurity()) {

        options = BigQueryOptions.newBuilder()
        .setProjectId(meta.getProjectId())
        .build();
      } else {
      options = BigQueryOptions.newBuilder()
      .setProjectId(meta.getProjectId())
      .setCredentials(GoogleCredentials.fromStream(
        new FileInputStream(meta.getCredentialsPath()))
      ).build();
      }
      data.bigquery = options.getService();

      DatasetInfo datasetInfo = Dataset.of(meta.getDatasetName());
      Dataset dataset = data.bigquery.getDataset(datasetInfo.getDatasetId());

      if (dataset == null) {
          if (meta.getCreateDataset()) {
              System.out.println("Creating dataset");
              dataset = data.bigquery.create(datasetInfo);
          } else {
              System.out.println("Please create the dataset: " + meta.getDatasetName());
              System.exit(1);
          }
      }

      data.tableId = TableId.of(dataset.getDatasetId().getDataset(), meta.getTableName());
      Table table = data.bigquery.getTable(data.tableId);

      if (table == null) {
          if (meta.getCreateTable()) {
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

          // TODO copy fields and types from input stream

          fieldList.add(Field.of("Name", LegacySQLTypeName.STRING));
          fieldList.add(Field.of("Age", LegacySQLTypeName.INTEGER));


          Schema schema = Schema.of(fieldList);

          table = data.bigquery.create(TableInfo.of(data.tableId, StandardTableDefinition.of(schema)));
      } else {
          System.out.println("Please create the table: " + meta.getTableName());
          System.exit(1);
      }
  } // end table null if


} catch (IOException ioe) {
    //ioe.printStackTrace(System.out);
    logError("Error loading Google Credentials File",ioe);
}
      // initialise new builder
          data.batchCounter = 0;
          data.insertBuilder = InsertAllRequest.newBuilder(data.tableId);
      
      
    } // end if for first row (initialisation based on row data)

    // Do something to this row's data (create row for BigQuery, and append to current stream)
    data.inputRowMeta = getInputRowMeta();
    int numFields = data.inputRowMeta.getFieldNames().length;
    
    Map<String, Object> rowContent = new HashMap<String,Object>();
    for ( int i = 0; i < numFields; i++ ) {
        ValueMetaInterface valueMeta = data.inputRowMeta.getValueMeta( i );
        Object valueData = r[i];

        // Copy field name and value to BigQuery field
        // add field to row
        // TODO check for null values, and ignore (not supported by BigQuery)
        rowContent.put(valueMeta.getName(), valueData);
        
    }
    // send row to BigQuery via checkBatch
    checkBatch(rowContent);

    // Also copy rows to output

    //Object extraValue = new ValueMetaAndData();
    
    //Object[] outputRow = RowDataUtil.addValueData( r, data.outputRowMeta.size() - 1 );
    
    //putRow( data.outputRowMeta, outputRow ); 
    
    putRow( data.outputRowMeta, r );  
     

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "BigQueryStream.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  private void checkBatch(Map<String, Object> rowContent) {
      // maintain incrementor (if datavar not null)
      if (null != rowContent) {
          // if not null, add to batch
          data.insertBuilder.addRow(null,rowContent); // valid - checked source
      }
      // if over X rows in stream, end this batch and create another
      // if rowContent null, we're forcing the batch on final row in PDI
      if (null == rowContent || 500 == data.batchCounter) {
          if (data.batchCounter != 0) {
              // sanity check for non empty batch. E.g. if we have exactly 500 rows, the second batch is empty
              InsertAllRequest request = data.insertBuilder.build();
          
              InsertAllResponse response = data.bigquery.insertAll(request);
              if (response.hasErrors()) {
                // If any of the insertions failed, this lets you inspect the errors
                for (Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                  // inspect row error
                  //System.out.println(entry.toString());
                  logError("Error writing rows to BigQuery: " + entry.toString());
                }
                stopAll();
              }
          }

          // initialise new batch
          data.batchCounter = 0;
          data.insertBuilder = InsertAllRequest.newBuilder(data.tableId);
      } else {
          data.batchCounter++; // increment counter if row not null or we're not on a new stream batch
      }
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (BigQueryStreamMeta) smi;
    data = (BigQueryStreamData) sdi;

    if ( super.init( smi, sdi ) ) {
    	
//      int nrfields = meta.getFieldName().length;
//      data.fieldnrs = new int[nrfields];
//      data.values = new Object[nrfields];
//      data.counts = new long[nrfields];
//
//      data.fieldNames = new Object[nrfields];
//
//      data.type = new Object[nrfields];
//      data.max = new Object[nrfields];
//      data.min = new Object[nrfields];
//      data.distinctValues = new Object[nrfields];
//      data.allValues = new Object[nrfields];
//      data.allNumericValues = new Object[nrfields];
//      data.nullCount = new long[nrfields];
//      data.sum = new Object[nrfields];
//
//      data.mean = new Object[nrfields];
//      data.median = new Object[nrfields];
//      data.stddev = new Object[nrfields];
//      data.skewness = new Object[nrfields];
//      data.isBoolean = new Object[nrfields];

      return true;
    }
    return false;

  }
}