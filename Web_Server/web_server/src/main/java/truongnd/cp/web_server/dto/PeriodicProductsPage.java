package truongnd.cp.web_server.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@Builder
public class PeriodicProductsPage {
    List<PeriodicProductsDTO> periodicProductsDTOS;
    private Integer totalPage;
    private Integer currentPage;
    private Integer pageSize;
}
