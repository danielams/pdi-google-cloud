package org.itfactory.kettle;

import org.apache.commons.vfs2.*;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {

        FileObject csvFile = null;
        FileObject folder = null;
        try {
            FileSystemManager fsManager = VFS.getManager();
            /*csvFile = fsManager.resolveFile("gcs://cloud-test-123/sales_data1.csv");

            FileType type = csvFile.getType();
            long size = csvFile.getContent().getSize();

            System.out.println("TYPE: " + type + " SIZE: " + size);*/

            folder = fsManager.resolveFile("gcs://cloud-test-123/input/sales_data.csv");

            System.out.println("EXISTS: " + folder.exists());

            for (FileObject fileObject : folder.getChildren()) {
                System.out.println(fileObject.toString());
            }

            folder = fsManager.resolveFile("gcs://cloud-test-123/rwa/out");
            System.out.println("EXISTS: " + folder.exists());

            for (FileObject fileObject : folder.getChildren()) {
                System.out.println(fileObject.toString() + " type: " + fileObject.getType());
            }

            folder = fsManager.resolveFile("gcs://cloud-test-123/rwa/in");
            folder.createFolder();

        } catch (FileSystemException e) {
            e.printStackTrace();
        }
    }
}
