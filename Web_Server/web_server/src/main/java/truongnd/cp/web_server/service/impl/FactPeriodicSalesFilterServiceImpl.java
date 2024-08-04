package truongnd.cp.web_server.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import truongnd.cp.web_server.constant.PeriodicType;
import truongnd.cp.web_server.dto.*;
import truongnd.cp.web_server.entity.FactPeriodicFilter;
import truongnd.cp.web_server.repository.FactPeriodicSalesFilterRepository;
import truongnd.cp.web_server.service.FactPeriodicSalesFilterService;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class FactPeriodicSalesFilterServiceImpl implements FactPeriodicSalesFilterService {
    private final FactPeriodicSalesFilterRepository factPeriodicSalesFilterRepository;

    @Override
    public Optional<List<FactPeriodicFilter>> getFactPeriodicFilters(PeriodicSearchDTO dto) {
        return factPeriodicSalesFilterRepository.findFactPeriodicFilter(
                dto.getPlatformId(),
                dto.getCategoryId(),
                dto.getTimePeriod(),
                dto.getKeywords()
        );
    }

    @Override
    public PeriodicSummary calculatePeriodicSummary(List<FactPeriodicFilter> factPeriodicFilters) {
        if (factPeriodicFilters != null) {
            LocalDate mostRecentDate = factPeriodicFilters.stream()
                    .map(FactPeriodicFilter::getUpdatedDate)
                    .max(LocalDate::compareTo)
                    .orElse(null);

            long totalSold = factPeriodicFilters.stream()
                    .mapToLong(FactPeriodicFilter::getSold)
                    .sum();

            BigDecimal totalRevenue = factPeriodicFilters.stream()
                    .map(FactPeriodicFilter::getAmount)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);

            long totalProducts = factPeriodicFilters.stream()
                    .map(f -> f.getProductCode() + "_" + f.getPlatformId() + "_" + f.getSellerId())
                    .distinct()
                    .count();

            long totalSellers = factPeriodicFilters.stream()
                    .map(FactPeriodicFilter::getSellerId)
                    .distinct()
                    .count();

            return PeriodicSummary.builder()
                    .mostRecentDate(mostRecentDate)
                    .totalSold(totalSold)
                    .totalRevenue(totalRevenue)
                    .totalProducts(totalProducts)
                    .totalSellers(totalSellers)
                    .build();
        }
        return PeriodicSummary.builder().build();
    }

    @Override
    public List<ChartDTO> getCategoryChartData(String type, List<FactPeriodicFilter> factPeriodicFilters) {
        record CategoryKey(Long categoryId, String categoryName) {
        }

        if (type.equals(PeriodicType.SALES.getValue())) {
            Map<CategoryKey, Long> groupedData = factPeriodicFilters.stream()
                    .collect(Collectors.groupingBy(
                            filter -> new CategoryKey(filter.getCategoryId(), filter.getCategoryName()),
                            Collectors.summingLong(FactPeriodicFilter::getSold)
                    ));

            return groupedData.entrySet().stream()
                    .map(entry -> new ChartDTO(entry.getKey().categoryName(), entry.getValue()))
                    .sorted(Comparator.comparingLong(ChartDTO::getValue).reversed())
                    .limit(20)
                    .collect(Collectors.toList());
        } else {
            Map<CategoryKey, BigDecimal> groupedData = factPeriodicFilters.stream()
                    .collect(Collectors.groupingBy(
                            filter -> new CategoryKey(filter.getCategoryId(), filter.getCategoryName()),
                            Collectors.reducing(BigDecimal.ZERO, FactPeriodicFilter::getAmount, BigDecimal::add)
                    ));

            return groupedData.entrySet().stream()
                    .map(entry -> new ChartDTO(entry.getKey().categoryName(), entry.getValue().longValue()))
                    .sorted(Comparator.comparing(ChartDTO::getValue).reversed())
                    .limit(20)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public List<ChartDTO> getSellerChartData(String type, List<FactPeriodicFilter> factPeriodicFilters) {
        record SellerKey(Long sellerId, String sellerName) {
        }

        if (type.equals(PeriodicType.SALES.getValue())) {
            Map<SellerKey, Long> groupedData = factPeriodicFilters.stream()
                    .collect(Collectors.groupingBy(
                            filter -> new SellerKey(filter.getSellerId(), filter.getSellerName()),
                            Collectors.summingLong(FactPeriodicFilter::getSold)
                    ));

            return groupedData.entrySet().stream()
                    .map(entry -> new ChartDTO(entry.getKey().sellerName(), entry.getValue()))
                    .sorted(Comparator.comparingLong(ChartDTO::getValue).reversed())
                    .limit(20)
                    .collect(Collectors.toList());
        } else {
            Map<SellerKey, BigDecimal> groupedData = factPeriodicFilters.stream()
                    .collect(Collectors.groupingBy(
                            filter -> new SellerKey(filter.getSellerId(), filter.getSellerName()),
                            Collectors.reducing(BigDecimal.ZERO, FactPeriodicFilter::getAmount, BigDecimal::add)
                    ));

            return groupedData.entrySet().stream()
                    .map(entry -> new ChartDTO(entry.getKey().sellerName(), entry.getValue().longValue()))
                    .sorted(Comparator.comparing(ChartDTO::getValue).reversed())
                    .limit(20)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public List<ChartDTO> getBrandChartData(String type, List<FactPeriodicFilter> factPeriodicFilters) {
        record BrandKey(Long brandId, String brandName) {
        }

        if (type.equals(PeriodicType.SALES.getValue())) {
            Map<BrandKey, Long> groupedData = factPeriodicFilters.stream()
                    .collect(Collectors.groupingBy(
                            filter -> new BrandKey(filter.getBrandId(), filter.getBrandName()),
                            Collectors.summingLong(FactPeriodicFilter::getSold)
                    ));

            return groupedData.entrySet().stream()
                    .map(entry -> new ChartDTO(entry.getKey().brandName(), entry.getValue()))
                    .sorted(Comparator.comparingLong(ChartDTO::getValue).reversed())
                    .limit(15)
                    .collect(Collectors.toList());
        } else {
            Map<BrandKey, BigDecimal> groupedData = factPeriodicFilters.stream()
                    .collect(Collectors.groupingBy(
                            filter -> new BrandKey(filter.getBrandId(), filter.getBrandName()),
                            Collectors.reducing(BigDecimal.ZERO, FactPeriodicFilter::getAmount, BigDecimal::add)
                    ));

            return groupedData.entrySet().stream()
                    .map(entry -> new ChartDTO(entry.getKey().brandName(), entry.getValue().longValue()))
                    .sorted(Comparator.comparing(ChartDTO::getValue).reversed())
                    .limit(15)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public List<ChartDTO> getLocationChartData(String type, List<FactPeriodicFilter> factPeriodicFilters) {
        record LocationKey(Integer locationId, String locationName) {
        }

        if (type.equals(PeriodicType.SALES.getValue())) {
            Map<LocationKey, Long> groupedData = factPeriodicFilters.stream()
                    .collect(Collectors.groupingBy(
                            filter -> new LocationKey(filter.getLocationId(), filter.getLocationName()),
                            Collectors.summingLong(FactPeriodicFilter::getSold)
                    ));

            return groupedData.entrySet().stream()
                    .map(entry -> new ChartDTO(entry.getKey().locationName(), entry.getValue()))
                    .sorted(Comparator.comparingLong(ChartDTO::getValue).reversed())
                    .limit(10)
                    .collect(Collectors.toList());
        } else {
            Map<LocationKey, BigDecimal> groupedData = factPeriodicFilters.stream()
                    .collect(Collectors.groupingBy(
                            filter -> new LocationKey(filter.getLocationId(), filter.getLocationName()),
                            Collectors.reducing(BigDecimal.ZERO, FactPeriodicFilter::getAmount, BigDecimal::add)
                    ));

            return groupedData.entrySet().stream()
                    .map(entry -> new ChartDTO(entry.getKey().locationName(), entry.getValue().longValue()))
                    .sorted(Comparator.comparing(ChartDTO::getValue).reversed())
                    .limit(10)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public List<PriceRangeChartDTO> getPriceRangeChartData(List<FactPeriodicFilter> factPeriodicFilters) {
        record PriceRange(String name, BigDecimal lowerBound, BigDecimal upperBound) {}

        List<PriceRange> priceRanges = List.of(
                new PriceRange("< 10,000đ", BigDecimal.ZERO, new BigDecimal("10000")),
                new PriceRange("10,000 - 50,000đ", new BigDecimal("10000"), new BigDecimal("50000")),
                new PriceRange("50,000 - 100,000đ", new BigDecimal("50000"), new BigDecimal("100000")),
                new PriceRange("100,000 - 200,000đ", new BigDecimal("100000"), new BigDecimal("200000")),
                new PriceRange("200,000 - 500,000đ", new BigDecimal("200000"), new BigDecimal("500000")),
                new PriceRange("500,000 - 1,000,000đ", new BigDecimal("500000"), new BigDecimal("1000000")),
                new PriceRange("1,000,000 - 3,000,000đ", new BigDecimal("1000000"), new BigDecimal("3000000")),
                new PriceRange("> 3,000,000đ", new BigDecimal("3000000"), null)
        );

        Map<PriceRange, List<FactPeriodicFilter>> groupedData = factPeriodicFilters.stream()
                .collect(Collectors.groupingBy(
                        filter -> priceRanges.stream()
                                .filter(range -> range.upperBound == null
                                        ? filter.getProductPrice().compareTo(range.lowerBound) >= 0
                                        : filter.getProductPrice().compareTo(range.lowerBound) >= 0
                                        && filter.getProductPrice().compareTo(range.upperBound) < 0)
                                .findFirst()
                                .orElseThrow(() -> new IllegalStateException("Unexpected price range for product"))
                ));

        return groupedData.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(PriceRange::lowerBound)))
                .map(entry -> {
                    PriceRange range = entry.getKey();
                    List<FactPeriodicFilter> filters = entry.getValue();
                    long totalSold = filters.stream().mapToLong(FactPeriodicFilter::getSold).sum();

                    Map<String, Long> platformAmounts = filters.stream()
                            .collect(Collectors.groupingBy(
                                    FactPeriodicFilter::getPlatformName,
                                    Collectors.reducing(BigDecimal.ZERO, FactPeriodicFilter::getAmount, BigDecimal::add)
                            ))
                            .entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().longValue()));

                    return new PriceRangeChartDTO(
                            range.name(),
                            totalSold,
                            platformAmounts
                    );
                })
                .collect(Collectors.toList());
    }

    @Override
    public PeriodicProductsPage getGrowthProductsData(String type, Integer page, List<FactPeriodicFilter> factPeriodicFilters) {
        List<PeriodicProductsDTO> mappedData = factPeriodicFilters.stream()
                .map(filter -> PeriodicProductsDTO.builder()
                        .productName(filter.getProductName())
                        .productImage(filter.getProductImage())
                        .productUrl(filter.getProductUrl())
                        .productPrice(filter.getProductPrice())
                        .productReviewCount(filter.getProductReviewCount())
                        .productAllTimeSold(filter.getProductAllTimeSold())
                        .totalSold(filter.getSold())
                        .totalAmount(filter.getAmount())
                        .build())
                .toList();

        List<PeriodicProductsDTO> sortedList;
        List<PeriodicProductsDTO> paginatedList = List.of();
        int pageSize = 0;
        int totalPage = 0;
        int currentPage = 0;

        if (type.equals(PeriodicType.SALES.getValue())) {
            sortedList = mappedData.stream()
                    .sorted(Comparator.comparing(PeriodicProductsDTO::getTotalSold).reversed())
                    .toList();
        } else {
            sortedList = mappedData.stream()
                    .sorted(Comparator.comparing(PeriodicProductsDTO::getTotalAmount).reversed())
                    .toList();
        }

        if (!sortedList.isEmpty()) {
            pageSize = 10;
            totalPage = (int) Math.ceil((double) sortedList.size() / pageSize);
            currentPage = Math.min(page, totalPage);
            int fromIndex = (currentPage - 1) * pageSize;
            int toIndex = Math.min(fromIndex + pageSize, sortedList.size());

            paginatedList = sortedList.subList(fromIndex, toIndex);
        }

        return PeriodicProductsPage.builder()
                .currentPage(currentPage)
                .pageSize(pageSize)
                .totalPage(totalPage)
                .periodicProductsDTOS(paginatedList)
                .build();
    }

    @Override
    public List<ChartDTO> getAllPlatformsData(String type, List<FactPeriodicFilter> factPeriodicFilters) {
        record PlatformKey(Integer platformId, String platformName) {}
        if (type.equals(PeriodicType.SALES.getValue())) {
            Map<PlatformKey, Long> groupedData = factPeriodicFilters.stream()
                    .collect(Collectors.groupingBy(
                            filter -> new PlatformKey(filter.getPlatformId(), filter.getPlatformName()),
                            Collectors.summingLong(FactPeriodicFilter::getSold)
                    ));

            return groupedData.entrySet().stream()
                    .map(entry -> new ChartDTO(entry.getKey().platformName(), entry.getValue()))
                    .collect(Collectors.toList());
        } else {
            Map<PlatformKey, BigDecimal> groupedData = factPeriodicFilters.stream()
                    .collect(Collectors.groupingBy(
                            filter -> new PlatformKey(filter.getPlatformId(), filter.getPlatformName()),
                            Collectors.reducing(BigDecimal.ZERO, FactPeriodicFilter::getAmount, BigDecimal::add)
                    ));

            return groupedData.entrySet().stream()
                    .map(entry -> new ChartDTO(entry.getKey().platformName(), entry.getValue().longValue()))
                    .collect(Collectors.toList());
        }
    }
}
