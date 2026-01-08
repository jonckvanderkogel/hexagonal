package com.bullit.domain.port.driven.file;

import java.util.Optional;

public interface CsvRow {
    String field(String name);

    Optional<String> findField(String name);
}