package truongnd.cp.web_server.constant;

import lombok.Getter;

@Getter
public enum TimePeriod {
    _7_DAY("7 Ngày", "7-DAY"),
    _30_DAY("30 Ngày", "30-DAY"),
    _60_DAY("60 Ngày", "60-DAY");

    private final String name;
    private final String value;

    TimePeriod(String name, String value) {
        this.name = name;
        this.value = value;
    }
}
