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

public abstract class BaseHelperStepMeta extends StepMeta implements StepMetaInterface {
  private Class PKG;
  private GenericDialogParameter[] parameters;
  private Hashtable<String, Object> parameterValues = new Hashtable<String, Object>();

  public BaseHelperStepMeta(Class i18nClass, GenericDialogParameter[] parameters) {
    PKG = i18nClass;
    this.parameters = parameters;
    // hashtables default values to null on get anyway, so no set up needed
  }
  


  @Override
  /**
   * Clones this meta class instance in PDI
   */
  public Object clone() {
    return super.clone();
  }

  /**
   * Adds any additional fields to the stream
   */
  public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space) {
    // we don't add any, so leave blank
  }

  /**
   * Sets default metadata configuration
   */
  public void setDefault() {
    // do nothing by default = all set to null
    // TODO initialise parameters values to their default, or null if not set
  }

  /**
   * Loads step configuration from PDI ktr file XML
   */
  public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
    try {

      // TODO read from XML

    } catch (Exception e) {
      throw new KettleXMLException(BaseMessages.getString(PKG, "BigQueryStreamMeta.Exception.UnableToLoadStepInfo"), e);
    }
  }

  @Override
  /**
   * Returns the XML configuration of this step for saving in a ktr file
   */
  public String getXML() {
    StringBuffer retval = new StringBuffer(300);

    // TODO output XML from parameter values

    return retval.toString();
  }

  /**
   * Reads the configuration of this step from a repository
   */
  public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases)
      throws KettleException {

    try {
      
      // TODO read rep from XML

    } catch (Exception e) {
      throw new KettleException(
          BaseMessages.getString(PKG, "FieldAnalysisMeta.Exception.UnexpectedErrorWhileReadingStepInfo"), e);
    }

  }

  /**
   * Saves the configuration of this step to a repository
   */
  public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step)
      throws KettleException {
    try {

      // TODO save rep to XML

    } catch (KettleException e) {
      throw new KettleException(
          BaseMessages.getString(PKG, PKG.getName() + ".Exception.UnableToSaveStepInfo") + id_step, e);
    }
  }

  /**
   * Validates this step's configuration
   */
  public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
      IMetaStore metaStore) {

    CheckResult cr;

    // TODO check for required fields, and permissableValues (if set)

    if (input.length > 0) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK,
          BaseMessages.getString(PKG, PKG.getName() + ".CheckResult.StepReceiveInfo.DialogMessage"), stepMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString(PKG, PKG.getName() + ".CheckResult.NoInputReceived.DialogMessage"), stepMeta);
      remarks.add(cr);
    }

  }

  public void set(String paramId, Object value) {
    parameterValues.put(paramId,value);
  }

  public Object get(String paramId) {
    return parameterValues.get(paramId);
  }

  public abstract StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans trans );

  public abstract StepDataInterface getStepData();
}