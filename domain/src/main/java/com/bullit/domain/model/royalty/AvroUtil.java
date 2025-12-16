package com.bullit.domain.model.royalty;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class AvroUtil {
    public static BigDecimal scale(BigDecimal value) {
        return value.setScale(2, RoundingMode.HALF_UP);
    }
}
