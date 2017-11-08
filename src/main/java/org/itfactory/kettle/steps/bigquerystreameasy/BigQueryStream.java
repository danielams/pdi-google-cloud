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

package org.itfactory.kettle.steps.bigquerystreameasy;

import org.itfactory.kettle.BaseHelperStep;
import org.itfactory.kettle.BaseHelperStepMeta;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.auth.oauth2.GoogleCredentials;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.List;
import java.util.ArrayList;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * BigQuery Stream loading Output Step
 *
 * @author afowler
 * @since 06-11-2017
 */
public class BigQueryStream extends BaseHelperStep {
  private static Class<?> PKG = BigQueryStreamMeta.class; // for i18n purposes, needed by Translator2!!

  private BaseHelperStepMeta meta;
  private BigQueryStreamData data;

  /**
   * Standard constructor
   */
  public BigQueryStream( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public boolean handleInit() {
    // do nothing special here
    return true;
  }

  public void beforeFirstRow() throws KettleException {

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
      if ( ((Boolean) meta.get( "useContainerSecurity" ) ).booleanValue() ) {
        options = BigQueryOptions.newBuilder().setProjectId( (String) meta.get( "projectId" ) ).build();
      } else {
        options = BigQueryOptions.newBuilder().setProjectId( (String) meta.get( "projectId" ) )
          .setCredentials( GoogleCredentials.fromStream( new FileInputStream( (String) meta.get ( "credentialsPath" ) ) ) ).build();
      }
      data.bigquery = options.getService();

      DatasetInfo datasetInfo = Dataset.of( (String) meta.get( "datasetName" ) );
      Dataset dataset = data.bigquery.getDataset( datasetInfo.getDatasetId() );

      if ( dataset == null ) {
        if ( ((Boolean) meta.get( "createDataset" )).booleanValue() )  {
          //System.out.println( "Creating dataset" );
          dataset = data.bigquery.create( datasetInfo );
        } else {
          System.out.println( "Please create the dataset: " + meta.get( "datasetName" ) );
          //System.exit( 1 );
        }
      }

      data.tableId = TableId.of( dataset.getDatasetId().getDataset(), (String) meta.get( "tableName" ) );
      Table table = data.bigquery.getTable( data.tableId );

      if ( table == null ) {
        if ( ((Boolean) meta.get( "createTable" ) ).booleanValue() ) {
          System.out.println( "Creating table" );
          List<Field> fieldList = new ArrayList<Field>();
          
          // sample age data csv

          // copy fields and types from input stream

          fieldList.add(Field.of("Name", LegacySQLTypeName.STRING));
          fieldList.add(Field.of("Age", LegacySQLTypeName.INTEGER));

          Schema schema = Schema.of(fieldList);

          table = data.bigquery.create(TableInfo.of(data.tableId, StandardTableDefinition.of(schema)));
        } else {
          System.out.println( "Please create the table: " + meta.get( "tableName" ) );
          System.exit( 1 );
        }
      } // end table null if

    } catch ( IOException ioe ) {
      logError( "Error loading Google Credentials File", ioe );
    }
    // initialise new builder
    data.batchCounter = 0;
    data.insertBuilder = InsertAllRequest.newBuilder( data.tableId );

  }

  public void handleRow( Object[] r ) throws KettleException {

    // Do something to this row's data (create row for BigQuery, and append to current stream)
    data.inputRowMeta = getInputRowMeta();
    int numFields = data.inputRowMeta.getFieldNames().length;

    Map<String, Object> rowContent = new HashMap<String, Object>();
    for ( int i = 0; i < numFields; i++ ) {
      ValueMetaInterface valueMeta = data.inputRowMeta.getValueMeta(i);
      Object valueData = r[i];

      // Copy field name and value to BigQuery field
      // add field to row
      // TODO check for null values, and ignore (not supported by BigQuery)
      rowContent.put( valueMeta.getName(), valueData );

    }
    // send row to BigQuery via checkBatch
    checkBatch( rowContent );

    // Also copy rows to output
    putRow( data.outputRowMeta, r );
  }

  public void afterLastRow() throws KettleException {
    checkBatch( null );
    // clean up connection and environment
    data.insertBuilder = null;
    data.tableId = null;
  }



  private void checkBatch( Map<String, Object> rowContent ) {
    // maintain incrementor (if datavar not null)
    if ( null != rowContent ) {
      // if not null, add to batch
      data.insertBuilder.addRow( null, rowContent ); // valid - checked source
    }
    // if over X rows in stream, end this batch and create another
    // if rowContent null, we're forcing the batch on final row in PDI
    if ( null == rowContent || 500 == data.batchCounter ) {
      if ( data.batchCounter != 0 ) {
        // sanity check for non empty batch. E.g. if we have exactly 500 rows, the second batch is empty
        InsertAllRequest request = data.insertBuilder.build();

        InsertAllResponse response = data.bigquery.insertAll( request );
        if ( response.hasErrors() ) {
          // If any of the insertions failed, this lets you inspect the errors
          for ( Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet() ) {
            // inspect row error
            //System.out.println(entry.toString());
            logError( "Error writing rows to BigQuery: " + entry.toString() );
          }
          stopAll();
        }
      }

      // initialise new batch
      data.batchCounter = 0;
      data.insertBuilder = InsertAllRequest.newBuilder( data.tableId );
    } else {
      data.batchCounter++; // increment counter if row not null or we're not on a new stream batch
    }
  }

}
