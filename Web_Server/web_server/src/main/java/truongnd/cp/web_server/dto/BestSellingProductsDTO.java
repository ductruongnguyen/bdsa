package truongnd.cp.web_server.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
@Builder
public class BestSellingProductsDTO {
    private String productName;
    private String productImage;
    private String productUrl;
    private BigDecimal productPrice;
    private Integer productReviewCount;
    private BigDecimal productRatingScore;
    private String categoryName;
    private String sellerName;
    private String brandName;
    private String locationName;
    private Long totalSold;
    private BigDecimal totalAmount;
}
