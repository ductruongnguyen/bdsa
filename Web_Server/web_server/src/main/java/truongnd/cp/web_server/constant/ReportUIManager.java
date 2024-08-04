package truongnd.cp.web_server.constant;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Builder(builderClassName = "Builder")
@Getter
public class ReportUIManager {
    private boolean showAllPlatform;
    private boolean showCategory;
    private boolean showSeller;
    private boolean showBrand;
    private boolean showLocation;
    private boolean showPriceRange;
    private boolean showProducts;

    // Custom builder to initialize fields to false and change specific fields to true
    public static class Builder {
        private boolean showAllPlatform = false;
        private boolean showCategory = false;
        private boolean showSeller = false;
        private boolean showBrand = false;
        private boolean showLocation = false;
        private boolean showPriceRange = false;
        private boolean showProducts = false;

        public Builder changeShowPlatform(boolean value) {
            this.showAllPlatform = value;
            return this;
        }

        public Builder changeShowCategory(boolean value) {
            this.showCategory = value;
            return this;
        }

        public Builder changeShowSeller(boolean value) {
            this.showSeller = value;
            return this;
        }

        public Builder changeShowBrand(boolean value) {
            this.showBrand = value;
            return this;
        }

        public Builder changeShowLocation(boolean value) {
            this.showLocation = value;
            return this;
        }

        public Builder changeShowPriceRange(boolean value) {
            this.showPriceRange = value;
            return this;
        }

        public Builder changeShowProducts(boolean value) {
            this.showProducts = value;
            return this;
        }

        public ReportUIManager build() {
            return new ReportUIManager(showAllPlatform, showCategory, showSeller, showBrand, showLocation, showPriceRange, showProducts);
        }
    }
}
