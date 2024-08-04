package truongnd.cp.web_server.dto;

import lombok.*;

import java.util.List;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BestSellingProductsPage {
    List<BestSellingProductsDTO> bestSellingProductsDTOS;
    private Integer totalPage;
    private Integer currentPage;
    private Integer pageSize;
}
