package com.bullit.application;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({
        "schema",
        "specificData",
        "getSchema",
        "getSpecificData"
})
public interface AvroSpecificRecordMixin {}