package truongnd.cp.web_server.dto;

import lombok.*;

import java.util.Map;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class PriceRangeChartDTO {
    private String priceRange;
    private Long totalSold;
    private Map<String, Long> platformAmounts;
}
