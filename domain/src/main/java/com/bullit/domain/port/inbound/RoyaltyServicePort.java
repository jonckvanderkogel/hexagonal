package com.bullit.domain.port.inbound;

import com.bullit.domain.model.royalty.RoyaltyReport;

import java.time.YearMonth;
import java.util.UUID;

public interface RoyaltyServicePort {
    RoyaltyReport generateMonthlyReport(UUID authorId, YearMonth period);
}