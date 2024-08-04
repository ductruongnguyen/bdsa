package truongnd.cp.web_server.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
@Builder
public class PeriodicProductsDTO {
    private String productName;
    private String productImage;
    private String productUrl;
    private BigDecimal productPrice;
    private Integer productReviewCount;
    private Integer productAllTimeSold;
    private Long totalSold;
    private BigDecimal totalAmount;
}
