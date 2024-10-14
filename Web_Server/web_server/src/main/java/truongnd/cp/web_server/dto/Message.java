package truongnd.cp.web_server.dto;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Message {
    private String role;
    private String content;
}
