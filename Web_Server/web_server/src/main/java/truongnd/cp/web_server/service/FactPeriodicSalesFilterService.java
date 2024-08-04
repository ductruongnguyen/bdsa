package truongnd.cp.web_server.service;

import truongnd.cp.web_server.dto.*;
import truongnd.cp.web_server.entity.FactPeriodicFilter;

import java.util.List;
import java.util.Optional;

public interface FactPeriodicSalesFilterService {
    Optional<List<FactPeriodicFilter>> getFactPeriodicFilters(PeriodicSearchDTO dto);
    PeriodicSummary calculatePeriodicSummary(List<FactPeriodicFilter> factPeriodicFilters);
    List<ChartDTO> getCategoryChartData(String type, List<FactPeriodicFilter> factPeriodicFilters);
    List<ChartDTO> getSellerChartData(String type, List<FactPeriodicFilter> factPeriodicFilters);
    List<ChartDTO> getBrandChartData(String type, List<FactPeriodicFilter> factPeriodicFilters);
    List<ChartDTO> getLocationChartData(String type, List<FactPeriodicFilter> factPeriodicFilters);
    List<PriceRangeChartDTO> getPriceRangeChartData(List<FactPeriodicFilter> factPeriodicFilters);
    PeriodicProductsPage getGrowthProductsData(String type, Integer page, List<FactPeriodicFilter> factPeriodicFilters);
    List<ChartDTO> getAllPlatformsData(String type, List<FactPeriodicFilter> factPeriodicFilters);
}
