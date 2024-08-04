package truongnd.cp.web_server.constant;

import lombok.Getter;

@Getter
public enum MenuItems {
    GROWTH_ANALYST(1, "Phân tích theo giai đoạn"),
    BEST_SELLING_PRODUCTS(2, "Sản phẩm bán chạy"),
    ACCOUNT(3, "Tài khoản"),
    USER_GUIDE(4, "Hướng dẫn sử dụng");

    private final int code;
    private final String value;

    MenuItems(int code, String value) {
        this.code = code;
        this.value = value;
    }

    public static String getValueByCode(int code) {
        for (MenuItems e : MenuItems.values()) {
            if (e.getCode() == code) {
                return e.getValue();
            }
        }
        throw new IllegalArgumentException("Invalid code: " + code);
    }
}
