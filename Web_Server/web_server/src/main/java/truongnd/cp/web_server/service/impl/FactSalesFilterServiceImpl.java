package truongnd.cp.web_server.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import truongnd.cp.web_server.dto.BestSellingFilterDTO;
import truongnd.cp.web_server.dto.BestSellingProductsDTO;
import truongnd.cp.web_server.dto.BestSellingProductsPage;
import truongnd.cp.web_server.entity.FactSaleFilter;
import truongnd.cp.web_server.repository.FactSaleFilterRepository;
import truongnd.cp.web_server.service.FactSalesFilterService;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class FactSalesFilterServiceImpl implements FactSalesFilterService {
    private final FactSaleFilterRepository factSaleFilterRepository;


    @Override
    public BestSellingProductsPage getBestSellingProductsData(BestSellingFilterDTO dto, Integer page) {
        Optional<List<FactSaleFilter>> factSaleFiltersOpt = factSaleFilterRepository.findFactSaleFilter(
                dto.getPlatformId(),
                dto.getCategoryId(),
                dto.getKeywords()
        );

        if (factSaleFiltersOpt.isPresent()) {
            List<FactSaleFilter> factSaleFilters = factSaleFiltersOpt.get();
            if(!CollectionUtils.isEmpty(factSaleFilters) ) {
                List<BestSellingProductsDTO> bestSellingProducts = factSaleFilters.stream()
                        .map(item -> BestSellingProductsDTO.builder()
                                .productName(item.getProductName())
                                .productImage(item.getProductImage())
                                .productUrl(item.getProductUrl())
                                .productPrice(item.getProductPrice())
                                .productReviewCount(item.getProductReviewCount())
                                .productRatingScore(item.getProductRatingScore())
                                .categoryName(item.getCategoryName())
                                .sellerName(item.getSellerName())
                                .brandName(item.getBrandName())
                                .locationName(item.getLocationName())
                                .totalSold(item.getTotalSold())
                                .totalAmount(item.getTotalAmount())
                                .build())
                        .toList();

                List<BestSellingProductsDTO> bestSellingSorted;

                if ("sold".equals(dto.getOrderBy())) {
                    bestSellingSorted = bestSellingProducts.stream()
                            .sorted(Comparator.comparing(BestSellingProductsDTO::getTotalSold).reversed())
                            .toList();
                } else {
                    bestSellingSorted = bestSellingProducts.stream()
                            .sorted(Comparator.comparing(BestSellingProductsDTO::getTotalAmount).reversed())
                            .toList();
                }

                int pageSize = 10;
                int totalPage = (int) Math.ceil((double) bestSellingSorted.size() / pageSize);
                int currentPage = Math.min(page, totalPage);
                int fromIndex = (currentPage - 1) * pageSize;
                int toIndex = Math.min(fromIndex + pageSize, bestSellingSorted.size());

                List<BestSellingProductsDTO> paginatedList = bestSellingSorted.subList(fromIndex, toIndex);

                return BestSellingProductsPage.builder()
                        .bestSellingProductsDTOS(paginatedList)
                        .currentPage(currentPage)
                        .totalPage(totalPage)
                        .pageSize(pageSize)
                        .build();
            }
        }
        return null;
    }
}
