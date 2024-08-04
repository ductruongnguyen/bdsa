package truongnd.cp.web_server.service;

import truongnd.cp.web_server.dto.BestSellingFilterDTO;
import truongnd.cp.web_server.dto.BestSellingProductsPage;

public interface FactSalesFilterService {
    BestSellingProductsPage getBestSellingProductsData(BestSellingFilterDTO dto, Integer page);
}
