package org.itfactory.kettle.google.vfs;

import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.url.UrlFileName;

/**
 * Created by puls3 on 04/07/2017.
 */
public class GCSFileName extends UrlFileName {
    public GCSFileName(String scheme, String hostName, int port, int defaultPort, String userName, String password, String path, FileType type, String queryString) {
        super(scheme, hostName, port, defaultPort, userName, password, path, type, queryString);
    }
}
