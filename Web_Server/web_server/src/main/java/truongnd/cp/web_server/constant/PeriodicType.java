package truongnd.cp.web_server.constant;

import lombok.Getter;

@Getter
public enum PeriodicType {
    REVENUE("revenue"),
    SALES("sales");

    private final String value;

    PeriodicType(String value) {
        this.value = value;
    }
}
