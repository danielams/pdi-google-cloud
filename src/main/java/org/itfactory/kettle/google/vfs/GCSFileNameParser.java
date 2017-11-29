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

package org.itfactory.kettle.google.vfs;

import org.apache.commons.vfs2.provider.URLFileNameParser;

/**
 * GCSFileNameParser
 *
 * @author asimoes
 * @since 15-11-2017
 */
public class GCSFileNameParser extends URLFileNameParser {

  private static final GCSFileNameParser INSTANCE = new GCSFileNameParser();

  public GCSFileNameParser() {
    super( 843 );
  }

  public static GCSFileNameParser getInstance() {
    return INSTANCE;
  }
}
