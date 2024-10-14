package truongnd.cp.web_server.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class ChatResponse {
    private List<Choice> choices;

    @Getter
    @Setter
    public static class Choice {
        private int index;
        private Message message;
    }
}
