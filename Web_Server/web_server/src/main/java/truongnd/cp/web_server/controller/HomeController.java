package truongnd.cp.web_server.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestParam;
import truongnd.cp.web_server.constant.MenuItems;
import truongnd.cp.web_server.constant.PeriodicType;
import truongnd.cp.web_server.constant.ReportUIManager;
import truongnd.cp.web_server.constant.TimePeriod;
import truongnd.cp.web_server.dto.*;
import truongnd.cp.web_server.entity.DimCategory;
import truongnd.cp.web_server.entity.DimPlatform;
import truongnd.cp.web_server.entity.FactPeriodicFilter;
import truongnd.cp.web_server.repository.DimCategoryRepository;
import truongnd.cp.web_server.repository.DimPlatformRepository;
import truongnd.cp.web_server.service.FactPeriodicSalesFilterService;
import truongnd.cp.web_server.service.FactSalesFilterService;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Controller
@RequiredArgsConstructor
public class HomeController {
    private final FactPeriodicSalesFilterService factPeriodicSalesFilterService;
    private final FactSalesFilterService factSalesFilterService;
    private final DimPlatformRepository platformRepository;
    private final DimCategoryRepository categoryRepository;

    @GetMapping("/")
    public String showHome(Model model,
                           @RequestParam(name = "menuItem", defaultValue = "1") Integer menuItem) {

        CustomOAuth2User user = (CustomOAuth2User) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        List<DimPlatform> platformList = platformRepository.findAll();

        model.addAttribute("username", user.getName());
        model.addAttribute("menuHeader", MenuItems.getValueByCode(menuItem));
        model.addAttribute("menuItem", menuItem);
        model.addAttribute("platforms", platformList);
        model.addAttribute("categories", null);
        model.addAttribute("periods", TimePeriod.values());
        model.addAttribute("showReport", false);
        model.addAttribute("dto", new PeriodicSearchDTO());
        return "home";
    }

    @GetMapping("/analyze")
    public String analyze(Model model,
                          @Valid @ModelAttribute("dto") PeriodicSearchDTO dto,
                          @RequestParam(name = "type", defaultValue = "revenue") String type,
                          @RequestParam(name = "page", defaultValue = "1") Integer page,
                          @RequestParam(name = "menuItem", defaultValue = "1") Integer menuItem,
                          @RequestParam(value = "currentPageBatch", defaultValue = "1") int currentPageBatch,
                          @RequestParam(value = "pagesPerBatch", defaultValue = "15") int pagesPerBatch) {

        CustomOAuth2User user = (CustomOAuth2User) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        // Transform the dto for SQL query
        PeriodicSearchDTO transformedDTO = PeriodicSearchDTO.builder()
                .platformId(dto.getPlatformId() == -99 ? null : dto.getPlatformId())
                .timePeriod(dto.getTimePeriod())
                .categoryId(dto.getCategoryId() == -99 ? null : dto.getCategoryId())
                .keywords(!StringUtils.hasText(dto.getKeywords()) ? null : dto.getKeywords())
                .build();

        // Init the general search query and add basic information to the UI
        Optional<List<FactPeriodicFilter>> factPeriodicFiltersOpt = factPeriodicSalesFilterService.getFactPeriodicFilters(transformedDTO);

        // Add init information to the UI
        List<DimPlatform> platformList = platformRepository.findAll();
        List<DimCategory> categoryList = categoryRepository.findAll();
        List<DimCategory> categories = categoryList.stream()
                .filter(category -> Objects.equals(dto.getPlatformId(), category.getPlatformId()) &&
                        category.getParentId() == null)
                .sorted((category1, category2) ->
                        category2.getCategoryId().compareTo(category1.getCategoryId()))
                .toList();

        model.addAttribute("username", user.getName());
        model.addAttribute("menuItem", menuItem);
        model.addAttribute("menuHeader", MenuItems.getValueByCode(menuItem));
        model.addAttribute("platforms", platformList);
        model.addAttribute("categories", categories);
        model.addAttribute("periods", TimePeriod.values());
        model.addAttribute("dto", dto);
        model.addAttribute("platformId", dto.getPlatformId());
        model.addAttribute("timePeriod", dto.getTimePeriod());
        model.addAttribute("categoryId", dto.getCategoryId());
        model.addAttribute("keywords", dto.getKeywords());
        model.addAttribute("showReport", false);
        model.addAttribute("type", type);

        List<FactPeriodicFilter> factPeriodicFilters = factPeriodicFiltersOpt.orElse(null);
        if (factPeriodicFilters == null || factPeriodicFilters.isEmpty()) {
            return "home";
        }

        this.addSummaryInformationToUI(model, dto, factPeriodicFilters);
        // Add price range chart
        List<PriceRangeChartDTO> priceRangeData = factPeriodicSalesFilterService.getPriceRangeChartData(factPeriodicFilters);
        model.addAttribute("chartLabel", Objects.equals(type, PeriodicType.REVENUE.getValue()) ? "Doanh thu (VND)" : "Lượt bán");
        model.addAttribute("priceRangeData", priceRangeData);

        // return the platforms report
        if (transformedDTO.getPlatformId() == null) {
            List<ChartDTO> allPlatformData = factPeriodicSalesFilterService.getAllPlatformsData(type, factPeriodicFilters);
            model.addAttribute("showReport", true);
            model.addAttribute("reportManager", ReportUIManager.builder()
                    .changeShowPlatform(true)
                    .changeShowPriceRange(true)
                    .build());
            model.addAttribute("allPlatformData", allPlatformData);
            return "home";
        }

        // calculate metrics for Category Chart Section
        List<ChartDTO> categoriesData = factPeriodicSalesFilterService.getCategoryChartData(type, factPeriodicFiltersOpt.orElse(null));
        List<ChartDTO> sellersData = factPeriodicSalesFilterService.getSellerChartData(type, factPeriodicFiltersOpt.orElse(null));
        int countSellers = sellersData.stream().map(ChartDTO::getName).distinct().toList().size();
        List<ChartDTO> brandsData = factPeriodicSalesFilterService.getBrandChartData(type, factPeriodicFiltersOpt.orElse(null));
        int countBrands = brandsData.stream().map(ChartDTO::getName).distinct().toList().size();
        List<ChartDTO> locationsData = factPeriodicSalesFilterService.getLocationChartData(type, factPeriodicFiltersOpt.orElse(null));
        int countLocations = locationsData.stream().map(ChartDTO::getName).distinct().toList().size();
        PeriodicProductsPage productsPage = factPeriodicSalesFilterService.getGrowthProductsData(type, page, factPeriodicFiltersOpt.orElse(null));
        List<PeriodicProductsDTO> productsData = productsPage.getPeriodicProductsDTOS();

        // Pagination
        int totalPage = productsPage.getTotalPage();
        int currentPage = productsPage.getCurrentPage();
        int pageSize = productsPage.getPageSize();

        int startPage = (currentPageBatch - 1) * pagesPerBatch + 1;
        int endPage = Math.min(currentPageBatch * pagesPerBatch, totalPage);

        model.addAttribute("categoriesData", categoriesData);
        if (countSellers > 1)
            model.addAttribute("sellersData", sellersData);
        if (countBrands > 1)
            model.addAttribute("brandsData", brandsData);
        if (countLocations > 1)
            model.addAttribute("locationsData", locationsData);

        model.addAttribute("productsData", productsData);
        model.addAttribute("totalPage", totalPage);
        model.addAttribute("currentPage", currentPage);
        model.addAttribute("pageSize", pageSize);
        model.addAttribute("currentPageBatch", currentPageBatch);
        model.addAttribute("pagesPerBatch", pagesPerBatch);
        model.addAttribute("startPage", startPage);
        model.addAttribute("endPage", endPage);
        model.addAttribute("showReport", true);
        model.addAttribute("reportManager", ReportUIManager.builder()
                .changeShowCategory(true)
                .changeShowSeller(countSellers > 1)
                .changeShowBrand(countBrands > 1)
                .changeShowLocation(countLocations > 1)
                .changeShowPriceRange(true)
                .changeShowProducts(true)
                .build());

        return "home";
    }

    private void addSummaryInformationToUI(Model model, PeriodicSearchDTO dto, List<FactPeriodicFilter> factPeriodicFilters) {
        // calculate metrics for Summary section
        PeriodicSummary periodicSummary = factPeriodicSalesFilterService.calculatePeriodicSummary(factPeriodicFilters);

        int day = Integer.parseInt(dto.getTimePeriod().split("-")[0]);
        LocalDate toDate = periodicSummary.getMostRecentDate();
        LocalDate fromDate = toDate.minusDays(day);

        // Bind all values to the UI
        model.addAttribute("fromDate", fromDate);
        model.addAttribute("toDate", toDate);

        model.addAttribute("totalRevenue", periodicSummary.getTotalRevenue());
        model.addAttribute("totalSold", periodicSummary.getTotalSold());
        model.addAttribute("totalProducts", periodicSummary.getTotalProducts());
        model.addAttribute("totalSellers", periodicSummary.getTotalSellers());
    }

    @GetMapping("/best-selling")
    public String bestSelling(Model model,
                              @Valid @ModelAttribute("dto") BestSellingFilterDTO dto,
                              @RequestParam(name = "page", defaultValue = "1") Integer page,
                              @RequestParam(name = "menuItem", defaultValue = "2") Integer menuItem,
                              @RequestParam(value = "currentPageBatch", defaultValue = "1") int currentPageBatch,
                              @RequestParam(value = "pagesPerBatch", defaultValue = "15") int pagesPerBatch) {

        // init binding to the UI
        CustomOAuth2User user = (CustomOAuth2User) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        List<DimPlatform> platformList = platformRepository.findAll();
        List<DimCategory> categoryList = categoryRepository.findAll();
        List<DimCategory> categories = categoryList.stream()
                .filter(category -> Objects.equals(dto.getPlatformId(), category.getPlatformId()) &&
                        category.getParentId() == null)
                .sorted((category1, category2) ->
                        category2.getCategoryId().compareTo(category1.getCategoryId()))
                .toList();
        model.addAttribute("username", user.getName());
        model.addAttribute("menuItem", menuItem);
        model.addAttribute("menuHeader", MenuItems.getValueByCode(menuItem));
        model.addAttribute("platforms", platformList);
        model.addAttribute("categories", categories);
        model.addAttribute("dto", dto);

        if (dto.getPlatformId() == -99 || "-99".equals(dto.getOrderBy())) {
            model.addAttribute("showData", false);
            return "best-selling";
        }

        BestSellingFilterDTO transferDTO = BestSellingFilterDTO.builder()
                .platformId(dto.getPlatformId())
                .categoryId(dto.getCategoryId() == -99L ? null : dto.getCategoryId())
                .orderBy(dto.getOrderBy())
                .keywords(!StringUtils.hasText(dto.getKeywords()) ? null : dto.getKeywords())
                .build();

        // Get products list
        BestSellingProductsPage productsPage = factSalesFilterService.getBestSellingProductsData(transferDTO, page);

        if (productsPage == null) {
            model.addAttribute("showData", false);
            return "best-selling";
        }

        // Pagination
        int totalPage = productsPage.getTotalPage();
        int currentPage = productsPage.getCurrentPage();
        int pageSize = productsPage.getPageSize();

        int startPage = (currentPageBatch - 1) * pagesPerBatch + 1;
        int endPage = Math.min(currentPageBatch * pagesPerBatch, totalPage);

        model.addAttribute("showData", true);
        model.addAttribute("totalPage", totalPage);
        model.addAttribute("currentPage", currentPage);
        model.addAttribute("pageSize", pageSize);
        model.addAttribute("productsData", productsPage.getBestSellingProductsDTOS());
        model.addAttribute("currentPageBatch", currentPageBatch);
        model.addAttribute("pagesPerBatch", pagesPerBatch);
        model.addAttribute("startPage", startPage);
        model.addAttribute("endPage", endPage);

        model.addAttribute("platformId", dto.getPlatformId());
        model.addAttribute("categoryId", dto.getCategoryId());
        model.addAttribute("orderBy", dto.getOrderBy());
        model.addAttribute("keywords", dto.getKeywords());

        return "best-selling";
    }

    @GetMapping("/user-profile")
    public String bestSelling(Model model,
                              @RequestParam(name = "menuItem", defaultValue = "3") Integer menuItem) {

        CustomOAuth2User user = (CustomOAuth2User) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        model.addAttribute("username", user.getName());
        model.addAttribute("user", user);
        model.addAttribute("menuItem", menuItem);
        model.addAttribute("menuHeader", MenuItems.getValueByCode(menuItem));

        return "user-profile";
    }

}
