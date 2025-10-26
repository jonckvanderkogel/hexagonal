package com.bullit.domain.port.driven;

import com.bullit.domain.model.royalty.Sale;

public interface SaleRepositoryPort {
    Sale addSale(Sale sale);
}
