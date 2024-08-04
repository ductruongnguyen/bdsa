package truongnd.cp.web_server.dto;

import lombok.*;

@Setter
@Getter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class BestSellingFilterDTO {
    @Builder.Default
    private Integer platformId = -99;
    @Builder.Default
    private Long categoryId = -99L;
    @Builder.Default
    private String orderBy = "-99";
    private String keywords;
}
