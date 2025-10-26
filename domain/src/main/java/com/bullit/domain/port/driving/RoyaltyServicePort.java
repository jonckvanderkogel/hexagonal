package com.bullit.domain.port.driving;

import com.bullit.domain.model.royalty.RoyaltyReport;
import com.bullit.domain.model.royalty.Sale;

import java.math.BigDecimal;
import java.time.YearMonth;
import java.util.UUID;

public interface RoyaltyServicePort {
    RoyaltyReport generateMonthlyReport(UUID authorId, YearMonth period);
    Sale createSale(UUID bookId, int units, BigDecimal amountEur);
}