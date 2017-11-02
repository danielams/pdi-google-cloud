package org.itfactory.kettle.google.bigquery;

import java.util.ArrayList;
import java.util.List;

public enum SupportedLoadFormat {
    CSV("Csv"),
    JSON("Json"),
    AVRO("Avro");

    private final String name;

    SupportedLoadFormat(String name) {
        this.name = name;
    }

    String formatName() {
        return name;
    }

    public static String[] getFormats() {
        List<String> formats = new ArrayList<String>();
        for(SupportedLoadFormat slf: SupportedLoadFormat.values()) {
            formats.add(slf.formatName());
        }

        return formats.toArray(new String[formats.size()]);
    }
}
