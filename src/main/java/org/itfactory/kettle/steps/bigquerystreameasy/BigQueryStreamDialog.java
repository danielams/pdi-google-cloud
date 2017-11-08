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

import org.itfactory.kettle.BaseHelperStepDialog;

import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;

import org.eclipse.swt.widgets.Shell;

/**
 * Dialog box for the BigQuery stream loading step
 * 
 * @author afowler
 * @since 06-11-2017
 */
public class BigQueryStreamDialog extends BaseHelperStepDialog {
  private static Class<?> PKG = BigQueryStreamMeta.class; // for i18n purposes, needed by Translator2!!

  /**
   * Standard PDI dialog constructor
   */
  public BigQueryStreamDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname, PKG );
  }

}