package org.itfactory.kettle.google.vfs;

import org.apache.commons.vfs2.provider.URLFileNameParser;

/**
 * Created by puls3 on 04/07/2017.
 */
public class GCSFileNameParser extends URLFileNameParser {

    private static final GCSFileNameParser INSTANCE = new GCSFileNameParser();

    public GCSFileNameParser() {
        super(843);
    }

    public static GCSFileNameParser getInstance() {
        return INSTANCE;
    }
}
