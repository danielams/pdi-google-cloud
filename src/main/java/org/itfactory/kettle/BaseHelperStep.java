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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Base Helper Step with utility methods. Breaks out parts of processRow typically implemented with functions,
 * for ease of development.
 *
 * @author afowler
 * @since 08-11-2017
 */
public abstract class BaseHelperStep extends BaseStep implements StepInterface {
  private StepMetaInterface meta;
  private StepDataInterface data;

  /**
   * Standard constructor
   */
  public BaseHelperStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans) {
    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
  }

  /**
   * Processes a single row in the PDI stream
   */
  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
    meta =  smi;
    data = sdi;

    Object[] r = getRow(); // get row, set busy!

    if (r == null) {
      afterLastRow();

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      beforeFirstRow();

    } // end if for first row (initialisation based on row data)

    handleRow(r);

    if (checkFeedback(getLinesRead())) {
      if (log.isBasic()) {
        logBasic( "Read lines: " + getLinesRead() );
      }
    }

    return true;
  }

  /**
   * Initialises the data for the step (meta data and runtime data)
   */
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    meta = smi;
    data = sdi;

    if ( super.init( smi, sdi ) ) {
      return handleInit();
    }
    return false;
  }

  /**
   * Handles any initialisation once Step Meta Interface and Step Data Interface have been set. 
   * 
   * Used to configure the step from meta configuration. E.g. set up a connection.
   * 
   * @return true if initialised successfully
   */
  public abstract boolean handleInit();

  /**
   * Called by processRow if we are just before the first row to be processed
   * 
   * Useful for configuring the step from actual data, rather than meta configuration. E.g. inspect input fields
   */
  public abstract void beforeFirstRow() throws KettleException;

  /**
   * Called by processRow for each actual row value, once step meta interface and step data interface are set
   * 
   * @param r An array of Objects constituting the field values for this single row
   */
  public abstract void handleRow(Object[] r) throws KettleException;

  /**
   * Called by processRow after the last row is processed (r==null in processRow)
   */
  public abstract void afterLastRow() throws KettleException;
}