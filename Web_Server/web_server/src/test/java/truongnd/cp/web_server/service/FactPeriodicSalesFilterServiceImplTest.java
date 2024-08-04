package truongnd.cp.web_server.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import truongnd.cp.web_server.dto.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import truongnd.cp.web_server.constant.PeriodicType;
import truongnd.cp.web_server.entity.FactPeriodicFilter;
import truongnd.cp.web_server.repository.FactPeriodicSalesFilterRepository;
import truongnd.cp.web_server.service.impl.FactPeriodicSalesFilterServiceImpl;

public class FactPeriodicSalesFilterServiceImplTest {

    @Mock
    private FactPeriodicSalesFilterRepository factPeriodicSalesFilterRepository;

    @InjectMocks
    private FactPeriodicSalesFilterServiceImpl factPeriodicSalesFilterService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testCalculatePeriodicSummary() {
        // Prepare mock data
        FactPeriodicFilter filter1 = new FactPeriodicFilter();
        filter1.setUpdatedDate(LocalDate.of(2023, 7, 1));
        filter1.setSold(100L);
        filter1.setAmount(new BigDecimal("1000"));
        filter1.setProductCode("P1");
        filter1.setPlatformId(1);
        filter1.setSellerId(1L);

        FactPeriodicFilter filter2 = new FactPeriodicFilter();
        filter2.setUpdatedDate(LocalDate.of(2023, 7, 2));
        filter2.setSold(150L);
        filter2.setAmount(new BigDecimal("1500"));
        filter2.setProductCode("P2");
        filter2.setPlatformId(2);
        filter2.setSellerId(2L);

        List<FactPeriodicFilter> filters = List.of(filter1, filter2);

        // Call the method to be tested
        PeriodicSummary summary = factPeriodicSalesFilterService.calculatePeriodicSummary(filters);

        // Verify the results
        assertEquals(LocalDate.of(2023, 7, 2), summary.getMostRecentDate());
        assertEquals(250, summary.getTotalSold());
        assertEquals(new BigDecimal("2500"), summary.getTotalRevenue());
        assertEquals(2, summary.getTotalProducts());
        assertEquals(2, summary.getTotalSellers());
    }

    @Test
    void testCalculatePeriodicSummaryEmptyList() {
        // Call the method with an empty list
        PeriodicSummary summary = factPeriodicSalesFilterService.calculatePeriodicSummary(List.of());

        // Verify the results
        assertNull(summary.getMostRecentDate());
        assertEquals(0, summary.getTotalSold());
        assertEquals(BigDecimal.ZERO, summary.getTotalRevenue());
        assertEquals(0, summary.getTotalProducts());
        assertEquals(0, summary.getTotalSellers());
    }

    @Test
    void testCalculatePeriodicSummaryNullList() {
        // Call the method with a null list
        PeriodicSummary summary = factPeriodicSalesFilterService.calculatePeriodicSummary(null);

        // Verify the results
        assertNull(summary.getMostRecentDate());
        assertNull(summary.getTotalSold());
        assertNull(summary.getTotalRevenue());
        assertNull(summary.getTotalProducts());
        assertNull(summary.getTotalSellers());
    }

    @Test
    void testGetCategoryChartDataRevenue() {
        // Prepare mock data
        FactPeriodicFilter filter1 = new FactPeriodicFilter();
        filter1.setCategoryId(1L);
        filter1.setCategoryName("Category1");
        filter1.setAmount(new BigDecimal("1000"));

        FactPeriodicFilter filter2 = new FactPeriodicFilter();
        filter2.setCategoryId(1L);
        filter2.setCategoryName("Category1");
        filter2.setAmount(new BigDecimal("1500"));

        FactPeriodicFilter filter3 = new FactPeriodicFilter();
        filter3.setCategoryId(2L);
        filter3.setCategoryName("Category2");
        filter3.setAmount(new BigDecimal("2000"));

        List<FactPeriodicFilter> filters = List.of(filter1, filter2, filter3);

        // Call the method to be tested
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getCategoryChartData(PeriodicType.REVENUE.getValue(), filters);

        // Verify the results
        assertEquals(2, chartData.size());
        assertEquals("Category1", chartData.get(0).getName());
        assertEquals(2500L, chartData.get(0).getValue());
        assertEquals("Category2", chartData.get(1).getName());
        assertEquals(2000L, chartData.get(1).getValue());
    }

    @Test
    void testGetCategoryChartDataEmptyList() {
        // Call the method with an empty list
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getCategoryChartData(PeriodicType.SALES.getValue(), List.of());

        // Verify the results
        assertTrue(chartData.isEmpty());
    }

    @Test
    void testGetSellerChartDataSales() {
        // Prepare mock data
        FactPeriodicFilter filter1 = new FactPeriodicFilter();
        filter1.setSellerId(1L);
        filter1.setSellerName("Seller1");
        filter1.setSold(100L);

        FactPeriodicFilter filter2 = new FactPeriodicFilter();
        filter2.setSellerId(1L);
        filter2.setSellerName("Seller1");
        filter2.setSold(150L);

        FactPeriodicFilter filter3 = new FactPeriodicFilter();
        filter3.setSellerId(2L);
        filter3.setSellerName("Seller2");
        filter3.setSold(200L);

        List<FactPeriodicFilter> filters = List.of(filter1, filter2, filter3);

        // Call the method to be tested
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getSellerChartData(PeriodicType.SALES.getValue(), filters);

        // Verify the results
        assertEquals(2, chartData.size());
        assertEquals("Seller1", chartData.get(0).getName());
        assertEquals(250, chartData.get(0).getValue());
        assertEquals("Seller2", chartData.get(1).getName());
        assertEquals(200, chartData.get(1).getValue());
    }

    @Test
    void testGetSellerChartDataRevenue() {
        // Prepare mock data
        FactPeriodicFilter filter1 = new FactPeriodicFilter();
        filter1.setSellerId(1L);
        filter1.setSellerName("Seller1");
        filter1.setAmount(new BigDecimal("1000"));

        FactPeriodicFilter filter2 = new FactPeriodicFilter();
        filter2.setSellerId(1L);
        filter2.setSellerName("Seller1");
        filter2.setAmount(new BigDecimal("1500"));

        FactPeriodicFilter filter3 = new FactPeriodicFilter();
        filter3.setSellerId(2L);
        filter3.setSellerName("Seller2");
        filter3.setAmount(new BigDecimal("2000"));

        List<FactPeriodicFilter> filters = List.of(filter1, filter2, filter3);

        // Call the method to be tested
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getSellerChartData(PeriodicType.REVENUE.getValue(), filters);

        // Verify the results
        assertEquals(2, chartData.size());
        assertEquals("Seller1", chartData.get(0).getName());
        assertEquals(2500L, chartData.get(0).getValue());
        assertEquals("Seller2", chartData.get(1).getName());
        assertEquals(2000L, chartData.get(1).getValue());
    }

    @Test
    void testGetSellerChartDataEmptyList() {
        // Call the method with an empty list
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getSellerChartData(PeriodicType.SALES.getValue(), List.of());

        // Verify the results
        assertTrue(chartData.isEmpty());
    }

    @Test
    void testGetBrandChartDataSales() {
        // Prepare mock data
        FactPeriodicFilter filter1 = new FactPeriodicFilter();
        filter1.setBrandId(1L);
        filter1.setBrandName("Brand1");
        filter1.setSold(100L);

        FactPeriodicFilter filter2 = new FactPeriodicFilter();
        filter2.setBrandId(1L);
        filter2.setBrandName("Brand1");
        filter2.setSold(150L);

        FactPeriodicFilter filter3 = new FactPeriodicFilter();
        filter3.setBrandId(2L);
        filter3.setBrandName("Brand2");
        filter3.setSold(200L);

        List<FactPeriodicFilter> filters = List.of(filter1, filter2, filter3);

        // Call the method to be tested
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getBrandChartData(PeriodicType.SALES.getValue(), filters);

        // Verify the results
        assertEquals(2, chartData.size());
        assertEquals("Brand1", chartData.get(0).getName());
        assertEquals(250L, chartData.get(0).getValue());
        assertEquals("Brand2", chartData.get(1).getName());
        assertEquals(200L, chartData.get(1).getValue());
    }

    @Test
    void testGetBrandChartDataRevenue() {
        // Prepare mock data
        FactPeriodicFilter filter1 = new FactPeriodicFilter();
        filter1.setBrandId(1L);
        filter1.setBrandName("Brand1");
        filter1.setAmount(new BigDecimal("1000"));

        FactPeriodicFilter filter2 = new FactPeriodicFilter();
        filter2.setBrandId(1L);
        filter2.setBrandName("Brand1");
        filter2.setAmount(new BigDecimal("1500"));

        FactPeriodicFilter filter3 = new FactPeriodicFilter();
        filter3.setBrandId(2L);
        filter3.setBrandName("Brand2");
        filter3.setAmount(new BigDecimal("2000"));

        List<FactPeriodicFilter> filters = List.of(filter1, filter2, filter3);

        // Call the method to be tested
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getBrandChartData(PeriodicType.REVENUE.getValue(), filters);

        // Verify the results
        assertEquals(2, chartData.size());
        assertEquals("Brand1", chartData.get(0).getName());
        assertEquals(2500L, chartData.get(0).getValue());
        assertEquals("Brand2", chartData.get(1).getName());
        assertEquals(2000L, chartData.get(1).getValue());
    }

    @Test
    void testGetBrandChartDataEmptyList() {
        // Call the method with an empty list
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getBrandChartData(PeriodicType.SALES.getValue(), List.of());

        // Verify the results
        assertTrue(chartData.isEmpty());
    }

    @Test
    void testGetLocationChartDataSales() {
        // Prepare mock data
        FactPeriodicFilter filter1 = new FactPeriodicFilter();
        filter1.setLocationId(1);
        filter1.setLocationName("Location1");
        filter1.setSold(100L);

        FactPeriodicFilter filter2 = new FactPeriodicFilter();
        filter2.setLocationId(1);
        filter2.setLocationName("Location1");
        filter2.setSold(150L);

        FactPeriodicFilter filter3 = new FactPeriodicFilter();
        filter3.setLocationId(2);
        filter3.setLocationName("Location2");
        filter3.setSold(200L);

        List<FactPeriodicFilter> filters = List.of(filter1, filter2, filter3);

        // Call the method to be tested
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getLocationChartData(PeriodicType.SALES.getValue(), filters);

        // Verify the results
        assertEquals(2, chartData.size());
        assertEquals("Location1", chartData.get(0).getName());
        assertEquals(250L, chartData.get(0).getValue());
        assertEquals("Location2", chartData.get(1).getName());
        assertEquals(200L, chartData.get(1).getValue());
    }

    @Test
    void testGetLocationChartDataRevenue() {
        // Prepare mock data
        FactPeriodicFilter filter1 = new FactPeriodicFilter();
        filter1.setLocationId(1);
        filter1.setLocationName("Location1");
        filter1.setAmount(new BigDecimal("1000"));

        FactPeriodicFilter filter2 = new FactPeriodicFilter();
        filter2.setLocationId(1);
        filter2.setLocationName("Location1");
        filter2.setAmount(new BigDecimal("1500"));

        FactPeriodicFilter filter3 = new FactPeriodicFilter();
        filter3.setLocationId(2);
        filter3.setLocationName("Location2");
        filter3.setAmount(new BigDecimal("2000"));

        List<FactPeriodicFilter> filters = List.of(filter1, filter2, filter3);

        // Call the method to be tested
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getLocationChartData(PeriodicType.REVENUE.getValue(), filters);

        // Verify the results
        assertEquals(2, chartData.size());
        assertEquals("Location1", chartData.get(0).getName());
        assertEquals(2500L, chartData.get(0).getValue());
        assertEquals("Location2", chartData.get(1).getName());
        assertEquals(2000L, chartData.get(1).getValue());
    }

    @Test
    void testGetLocationChartDataEmptyList() {
        // Call the method with an empty list
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getLocationChartData(PeriodicType.SALES.getValue(), List.of());

        // Verify the results
        assertTrue(chartData.isEmpty());
    }

    @Test
    void testGetPriceRangeChartData() {
        // Prepare mock data
        FactPeriodicFilter filter1 = new FactPeriodicFilter();
        filter1.setProductPrice(new BigDecimal("5000"));
        filter1.setSold(100L);
        filter1.setAmount(new BigDecimal("500000"));
        filter1.setPlatformName("Platform1");

        FactPeriodicFilter filter2 = new FactPeriodicFilter();
        filter2.setProductPrice(new BigDecimal("25000"));
        filter2.setSold(150L);
        filter2.setAmount(new BigDecimal("1000000"));
        filter2.setPlatformName("Platform2");

        FactPeriodicFilter filter3 = new FactPeriodicFilter();
        filter3.setProductPrice(new BigDecimal("150000"));
        filter3.setSold(200L);
        filter3.setAmount(new BigDecimal("1500000"));
        filter3.setPlatformName("Platform1");

        List<FactPeriodicFilter> filters = List.of(filter1, filter2, filter3);

        // Call the method to be tested
        List<PriceRangeChartDTO> chartData = factPeriodicSalesFilterService.getPriceRangeChartData(filters);

        // Verify the results
        assertEquals(3, chartData.size());

        PriceRangeChartDTO range1 = chartData.get(0);
        assertEquals("< 10,000đ", range1.getPriceRange());
        assertEquals(100L, range1.getTotalSold());
        assertEquals(1, range1.getPlatformAmounts().size());
        assertEquals(500000L, range1.getPlatformAmounts().get("Platform1"));

        PriceRangeChartDTO range2 = chartData.get(1);
        assertEquals("10,000 - 50,000đ", range2.getPriceRange());
        assertEquals(150L, range2.getTotalSold());
        assertEquals(1, range2.getPlatformAmounts().size());
        assertEquals(1000000L, range2.getPlatformAmounts().get("Platform2"));

        PriceRangeChartDTO range3 = chartData.get(2);
        assertEquals("100,000 - 200,000đ", range3.getPriceRange());
        assertEquals(200L, range3.getTotalSold());
        assertEquals(1, range3.getPlatformAmounts().size());
        assertEquals(1500000L, range3.getPlatformAmounts().get("Platform1"));
    }

    @Test
    void testGetPriceRangeChartDataEmptyList() {
        // Call the method with an empty list
        List<PriceRangeChartDTO> chartData = factPeriodicSalesFilterService.getPriceRangeChartData(List.of());

        // Verify the results
        assertTrue(chartData.isEmpty());
    }

    @Test
    void testGetGrowthProductsDataSales() {
        // Prepare mock data
        FactPeriodicFilter filter1 = new FactPeriodicFilter();
        filter1.setProductName("Product1");
        filter1.setProductImage("image1.jpg");
        filter1.setProductUrl("url1");
        filter1.setProductPrice(new BigDecimal("1000"));
        filter1.setProductReviewCount(10);
        filter1.setProductAllTimeSold(100);
        filter1.setSold(50L);
        filter1.setAmount(new BigDecimal("50000"));

        FactPeriodicFilter filter2 = new FactPeriodicFilter();
        filter2.setProductName("Product2");
        filter2.setProductImage("image2.jpg");
        filter2.setProductUrl("url2");
        filter2.setProductPrice(new BigDecimal("2000"));
        filter2.setProductReviewCount(20);
        filter2.setProductAllTimeSold(200);
        filter2.setSold(150L);
        filter2.setAmount(new BigDecimal("300000"));

        List<FactPeriodicFilter> filters = List.of(filter1, filter2);

        // Call the method to be tested
        PeriodicProductsPage resultPage = factPeriodicSalesFilterService.getGrowthProductsData(PeriodicType.SALES.getValue(), 1, filters);

        // Verify the results
        assertEquals(1, resultPage.getCurrentPage());
        assertEquals(10, resultPage.getPageSize());
        assertEquals(1, resultPage.getTotalPage());
        assertEquals(2, resultPage.getPeriodicProductsDTOS().size());

        PeriodicProductsDTO product1 = resultPage.getPeriodicProductsDTOS().get(0);
        assertEquals("Product2", product1.getProductName());
        assertEquals("image2.jpg", product1.getProductImage());
        assertEquals("url2", product1.getProductUrl());
        assertEquals(new BigDecimal("2000"), product1.getProductPrice());
        assertEquals(20, product1.getProductReviewCount());
        assertEquals(200, product1.getProductAllTimeSold());
        assertEquals(150L, product1.getTotalSold());
        assertEquals(new BigDecimal("300000"), product1.getTotalAmount());

        PeriodicProductsDTO product2 = resultPage.getPeriodicProductsDTOS().get(1);
        assertEquals("Product1", product2.getProductName());
        assertEquals("image1.jpg", product2.getProductImage());
        assertEquals("url1", product2.getProductUrl());
        assertEquals(new BigDecimal("1000"), product2.getProductPrice());
        assertEquals(10, product2.getProductReviewCount());
        assertEquals(100, product2.getProductAllTimeSold());
        assertEquals(50L, product2.getTotalSold());
        assertEquals(new BigDecimal("50000"), product2.getTotalAmount());
    }

    @Test
    void testGetGrowthProductsDataRevenue() {
        // Prepare mock data
        FactPeriodicFilter filter1 = new FactPeriodicFilter();
        filter1.setProductName("Product1");
        filter1.setProductImage("image1.jpg");
        filter1.setProductUrl("url1");
        filter1.setProductPrice(new BigDecimal("1000"));
        filter1.setProductReviewCount(10);
        filter1.setProductAllTimeSold(100);
        filter1.setSold(50L);
        filter1.setAmount(new BigDecimal("50000"));

        FactPeriodicFilter filter2 = new FactPeriodicFilter();
        filter2.setProductName("Product2");
        filter2.setProductImage("image2.jpg");
        filter2.setProductUrl("url2");
        filter2.setProductPrice(new BigDecimal("2000"));
        filter2.setProductReviewCount(20);
        filter2.setProductAllTimeSold(200);
        filter2.setSold(150L);
        filter2.setAmount(new BigDecimal("300000"));

        List<FactPeriodicFilter> filters = List.of(filter1, filter2);

        // Call the method to be tested
        PeriodicProductsPage resultPage = factPeriodicSalesFilterService.getGrowthProductsData(PeriodicType.REVENUE.getValue(), 1, filters);

        // Verify the results
        assertEquals(1, resultPage.getCurrentPage());
        assertEquals(10, resultPage.getPageSize());
        assertEquals(1, resultPage.getTotalPage());
        assertEquals(2, resultPage.getPeriodicProductsDTOS().size());

        PeriodicProductsDTO product1 = resultPage.getPeriodicProductsDTOS().get(0);
        assertEquals("Product2", product1.getProductName());
        assertEquals("image2.jpg", product1.getProductImage());
        assertEquals("url2", product1.getProductUrl());
        assertEquals(new BigDecimal("2000"), product1.getProductPrice());
        assertEquals(20, product1.getProductReviewCount());
        assertEquals(200, product1.getProductAllTimeSold());
        assertEquals(150L, product1.getTotalSold());
        assertEquals(new BigDecimal("300000"), product1.getTotalAmount());

        PeriodicProductsDTO product2 = resultPage.getPeriodicProductsDTOS().get(1);
        assertEquals("Product1", product2.getProductName());
        assertEquals("image1.jpg", product2.getProductImage());
        assertEquals("url1", product2.getProductUrl());
        assertEquals(new BigDecimal("1000"), product2.getProductPrice());
        assertEquals(10, product2.getProductReviewCount());
        assertEquals(100, product2.getProductAllTimeSold());
        assertEquals(50L, product2.getTotalSold());
        assertEquals(new BigDecimal("50000"), product2.getTotalAmount());
    }

    @Test
    void testGetGrowthProductsDataEmptyList() {
        // Call the method with an empty list
        PeriodicProductsPage resultPage = factPeriodicSalesFilterService.getGrowthProductsData(PeriodicType.SALES.getValue(), 1, List.of());

        // Verify the results
        assertEquals(0, resultPage.getCurrentPage());
        assertEquals(0, resultPage.getPageSize());
        assertEquals(0, resultPage.getTotalPage());
        assertTrue(resultPage.getPeriodicProductsDTOS().isEmpty());
    }

    @Test
    void testGetAllPlatformsDataSales() {
        // Prepare mock data
        FactPeriodicFilter filter1 = new FactPeriodicFilter();
        filter1.setPlatformId(1);
        filter1.setPlatformName("Platform1");
        filter1.setSold(100L);

        FactPeriodicFilter filter2 = new FactPeriodicFilter();
        filter2.setPlatformId(1);
        filter2.setPlatformName("Platform1");
        filter2.setSold(150L);

        FactPeriodicFilter filter3 = new FactPeriodicFilter();
        filter3.setPlatformId(2);
        filter3.setPlatformName("Platform2");
        filter3.setSold(200L);

        List<FactPeriodicFilter> filters = List.of(filter1, filter2, filter3);

        // Call the method to be tested
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getAllPlatformsData(PeriodicType.SALES.getValue(), filters);

        // Verify the results
        assertEquals(2, chartData.size());
        assertEquals("Platform2", chartData.get(0).getName());
        assertEquals(200L, chartData.get(0).getValue());
        assertEquals("Platform1", chartData.get(1).getName());
        assertEquals(250L, chartData.get(1).getValue());
    }

    @Test
    void testGetAllPlatformsDataRevenue() {
        // Prepare mock data
        FactPeriodicFilter filter1 = new FactPeriodicFilter();
        filter1.setPlatformId(1);
        filter1.setPlatformName("Platform1");
        filter1.setAmount(new BigDecimal("1000"));

        FactPeriodicFilter filter2 = new FactPeriodicFilter();
        filter2.setPlatformId(1);
        filter2.setPlatformName("Platform1");
        filter2.setAmount(new BigDecimal("1500"));

        FactPeriodicFilter filter3 = new FactPeriodicFilter();
        filter3.setPlatformId(2);
        filter3.setPlatformName("Platform2");
        filter3.setAmount(new BigDecimal("2000"));

        List<FactPeriodicFilter> filters = List.of(filter1, filter2, filter3);

        // Call the method to be tested
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getAllPlatformsData(PeriodicType.REVENUE.getValue(), filters);

        // Verify the results
        assertEquals(2, chartData.size());
        assertEquals("Platform2", chartData.get(0).getName());
        assertEquals(2000L, chartData.get(0).getValue());
        assertEquals("Platform1", chartData.get(1).getName());
        assertEquals(2500L, chartData.get(1).getValue());
    }

    @Test
    void testGetAllPlatformsDataEmptyList() {
        // Call the method with an empty list
        List<ChartDTO> chartData = factPeriodicSalesFilterService.getAllPlatformsData(PeriodicType.SALES.getValue(), List.of());

        // Verify the results
        assertTrue(chartData.isEmpty());
    }
}
