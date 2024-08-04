package truongnd.cp.web_server.dto;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDate;


@Getter
@Setter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class PeriodicSummary {
    private LocalDate mostRecentDate;
    private Long totalSold;
    private BigDecimal totalRevenue;
    private Long totalProducts;
    private Long totalSellers;
}
