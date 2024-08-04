package truongnd.cp.web_server.dto;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ChartDTO {
    private String name;
    private Long value;
}
