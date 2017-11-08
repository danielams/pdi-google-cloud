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

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.InsertAllRequest;

/**
 * Runtime transient data container for the PDI BigQuery stream step
 * 
 * @author afowler
 * @since 06-11-2017
 */
public class BigQueryStreamData extends BaseStepData implements StepDataInterface {
  public RowMetaInterface outputRowMeta;
  public RowMetaInterface inputRowMeta;

  // TODO use the below to handle specified fields only (not all stream fields)
  public int[] fieldnrs;
  public int nrfields;
  public Object[] values;
  public Object[] fieldNames;

  // bigquery stream state variables
  public BigQuery bigquery;
  public TableId tableId;
  public InsertAllRequest.Builder insertBuilder;
  public int batchCounter = 0;

  /**
   * Default constructor
   */
  public BigQueryStreamData() {
    super();
  }

}
