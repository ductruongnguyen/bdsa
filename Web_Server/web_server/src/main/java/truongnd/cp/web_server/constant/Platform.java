package truongnd.cp.web_server.constant;

import lombok.Getter;

@Getter
public enum Platform {
    SHOPEE("Shopee", 10),
    LAZADA("Lazada", 11),
    TIKI("Tiki", 12);

    private final String name;
    private final int value;

    Platform(String name, int value) {
        this.name = name;
        this.value = value;
    }
}
