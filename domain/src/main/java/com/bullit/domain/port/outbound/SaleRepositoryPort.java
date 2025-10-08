package com.bullit.domain.port.outbound;

import com.bullit.domain.model.royalty.Sale;

public interface SaleRepositoryPort {
    Sale addSale(Sale sale);
}
