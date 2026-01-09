package com.bullit.core.usecase;

import com.bullit.domain.event.SaleEvent;
import com.bullit.domain.port.driven.stream.StreamKey;

public class SaleEventKey implements StreamKey<SaleEvent> {
    @Override
    public Class<SaleEvent> payloadType() {
        return SaleEvent.class;
    }

    @Override
    public String apply(SaleEvent event) {
        return event.getId();
    }
}
