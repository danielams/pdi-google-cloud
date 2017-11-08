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

public class GenericDialogParameter {
  private String id;
  private String type;
  private boolean required;
  private Object[] permissableValues;
  private Object defaultValue;

  public GenericDialogParameter(String id,String type,boolean required) {
    this.id = id;
    this.type = type;
    this.required = required;
    this.permissableValues = null;
    this.defaultValue = null;
  }

  public GenericDialogParameter(String id, String type, boolean required,Object defaultValue) {
    this.id = id;
    this.type = type;
    this.required = required;
    this.permissableValues = null;
    this.defaultValue = defaultValue;
  }

  public GenericDialogParameter(String id,String type,boolean required,Object defaultValue,
      Object[] permissableValues) {
    this.id = id;
    this.type = type;
    this.required = required;
    this.permissableValues = permissableValues;
    this.defaultValue = defaultValue;
  }

  public String getId() {
    return id;
  }

  public String getType() {
    return type;
  }

  public boolean isRequired() {
    return required;
  }

  public Object[] getPermissableValues() {
    return permissableValues;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }
}