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

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.VFS;

/**
 * First test app for bulk loading data from google cloud storage to google bigquery
 */
public class App {
  public static void main( String[] args ) {
    FileObject folder = null;
    try {
      FileSystemManager fsManager = VFS.getManager();
      /*csvFile = fsManager.resolveFile("gcs://cloud-test-123/sales_data1.csv");
            
      FileType type = csvFile.getType();
      long size = csvFile.getContent().getSize();
            
      System.out.println("TYPE: " + type + " SIZE: " + size);*/

      folder = fsManager.resolveFile( "gcs://cloud-test-123/input/sales_data.csv" );

      System.out.println( "EXISTS: " + folder.exists() );

      for ( FileObject fileObject : folder.getChildren() ) {
        System.out.println( fileObject.toString() );
      }

      folder = fsManager.resolveFile( "gcs://cloud-test-123/rwa/out" );
      System.out.println( "EXISTS: " + folder.exists() );

      for ( FileObject fileObject : folder.getChildren() ) {
        System.out.println( fileObject.toString() + " type: " + fileObject.getType() );
      }

      folder = fsManager.resolveFile( "gcs://cloud-test-123/rwa/in" );
      folder.createFolder();

    } catch ( FileSystemException e ) {
      e.printStackTrace();
    }
  }
}
