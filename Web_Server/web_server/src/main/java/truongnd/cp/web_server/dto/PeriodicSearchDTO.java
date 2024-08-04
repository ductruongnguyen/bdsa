package truongnd.cp.web_server.dto;

import jakarta.validation.constraints.NotNull;
import lombok.*;

@Setter
@Getter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PeriodicSearchDTO {
    @NotNull
    private Integer platformId;
    @NotNull
    private String timePeriod;
    @NotNull
    private Long categoryId;
    private String keywords;
}
