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

import org.itfactory.kettle.GenericDialogParameter;
import org.itfactory.kettle.BaseHelperStepMeta;

import java.util.List;
import java.util.Hashtable;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;

import org.w3c.dom.Node;

@Step( id = "BigQueryStreamOutput",
image = "BigQueryStreamOutput.svg",
 i18nPackageName = "org.itfactory.kettle.steps.bigquerystream", name = "BigQueryStream.Name",
 description = "BigQueryStream.Description",
 categoryDescription = "i18n:org.pentaho.di.steps:StepCategory.Category.BigData" )
/**
 * Metadata (configuration) holding class for the BigQuery stream loading custom step
 * 
 * @author afowler
 * @since 06-11-2017
 */
public class BigQueryStreamMeta extends BaseHelperStepMeta {
  private static Class<?> PKG = BigQueryStreamMeta.class; // for i18n purposes, needed by Translator2!!

  public BigQueryStreamMeta() {
    super(PKG,
      new GenericDialogParameter[] {
        new GenericDialogParameter("useContainerSecurity","boolean",true,false),
        new GenericDialogParameter("credentialsPath","String",false),
        new GenericDialogParameter("projectId","String", false),
        new GenericDialogParameter("datasetName","String",true),
        new GenericDialogParameter("createDataset","boolean",true,true),
        new GenericDialogParameter("tableName","String",true),
        new GenericDialogParameter("createTable","boolean",true,true)
      }
    );
  }


  @Override
  /**
   * Returns a new instance of this step
   */
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans trans ) {
    return new BigQueryStream( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  @Override
  /**
   * Returns a new instance of step data
   */
  public StepDataInterface getStepData() {
    return new BigQueryStreamData();
  }
  

}