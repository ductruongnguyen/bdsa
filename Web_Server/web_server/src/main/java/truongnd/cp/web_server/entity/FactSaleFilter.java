package truongnd.cp.web_server.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDate;

@NamedNativeQuery(
        name = "selectFactSaleFilter",
        query = """
                with recent_sales as (select fs.*
                                      from ecommerce.fact_sales as fs
                                               inner join (select product_code, platform_id, seller_id, max(date_id) as max_date_id
                                                           from ecommerce.fact_sales
                                                           group by product_code, platform_id, seller_id) as recent
                                                          on fs.product_code = recent.product_code
                                                              and fs.platform_id = recent.platform_id
                                                              and fs.seller_id = recent.seller_id
                                                              and fs.date_id = recent.max_date_id
                                      where fs.units_sold > 0
                                        and fs.total_sales_amount > 0
                                        and (:platform_id is null or fs.platform_id = :platform_id)),
                     dim_category_tree as (select level_1.category_id as l1_id,
                                                  level_1.category_name,
                                                  level_2.category_id as l2_id,
                                                  level_2.category_name,
                                                  level_3.category_id as l3_id,
                                                  level_3.category_name,
                                                  level_4.category_id as l4_id,
                                                  level_4.category_name
                                           from (select *
                                                 from ecommerce.dim_category
                                                 where category_id = :category_id
                                                   and platform_id = :platform_id) as level_1
                                                    left join ecommerce.dim_category level_2
                                                              on level_1.category_id = level_2.parent_id
                                                    left join ecommerce.dim_category level_3 on level_2.category_id = level_3.parent_id
                                                    left join ecommerce.dim_category level_4
                                                              on level_3.category_id = level_4.parent_id),
                     dim_category_full as (select l1_id as category_id
                                           from dim_category_tree
                                           union
                                           select l2_id
                                           from dim_category_tree
                                           union
                                           select l3_id
                                           from dim_category_tree
                                           union
                                           select l4_id
                                           from dim_category_tree),
                     keywords AS (SELECT trim(unnest(string_to_array(:keyword, ','))) AS keyword)
                select rs.sale_id            as id,
                       rs.date_id            as dateId,
                       dd.date               as updatedDate,
                       rs.platform_id        as platformId,
                       dpl.platform_name     as platformName,
                       rs.category_id        as categoryId,
                       dc.category_name      as categoryName,
                       rs.seller_id          as sellerId,
                       ds.seller_name        as sellerName,
                       rs.product_code       as productCode,
                       dp.product_name       as productName,
                       dp.product_image      as productImage,
                       dp.product_url        as productUrl,
                       dp.price              as productPrice,
                       dp.review_count       as productReviewCount,
                       dp.rating_score       as productRatingScore,
                       rs.brand_id           as brandId,
                       db.brand_name         as brandName,
                       rs.location_id        as locationId,
                       dl.location_name      as locationName,
                       rs.units_sold         as totalSold,
                       rs.total_sales_amount as totalAmount
                from recent_sales as rs
                         inner join ecommerce.dim_date dd on rs.date_id = dd.date_id
                         inner join ecommerce.dim_category dc on dc.category_id = rs.category_id
                         inner join (select * from ecommerce.dim_product where flag = true) dp on dp.product_id = rs.product_id
                         inner join ecommerce.dim_location as dl on rs.location_id = dl.location_id
                         inner join ecommerce.dim_seller ds on ds.seller_id = rs.seller_id
                         inner join ecommerce.dim_brand db on db.brand_id = rs.brand_id
                         inner join ecommerce.dim_platform as dpl on rs.platform_id = dpl.platform_id
                where (:category_id is null or exists(select 1 from dim_category_full as dcf where rs.category_id = dcf.category_id))
                  and (:keyword is null or (SELECT COUNT(*)
                                            FROM keywords
                                            WHERE lower(dc.category_name) LIKE '%' || lower(keyword) || '%'
                                               OR lower(dp.product_name) LIKE '%' || lower(keyword) || '%'
                                               OR lower(dp.product_description) LIKE '%' || lower(keyword) || '%') > 0)
                """,
        resultSetMapping = "FactSaleFilterMapping"
)

@SqlResultSetMapping(
        name = "FactSaleFilterMapping",
        classes = @ConstructorResult(
                targetClass = FactSaleFilter.class,
                columns = {
                        @ColumnResult(name = "id", type = Long.class),
                        @ColumnResult(name = "dateId", type = Integer.class),
                        @ColumnResult(name = "updatedDate", type = LocalDate.class),
                        @ColumnResult(name = "platformId", type = Integer.class),
                        @ColumnResult(name = "platformName", type = String.class),
                        @ColumnResult(name = "categoryId", type = Long.class),
                        @ColumnResult(name = "categoryName", type = String.class),
                        @ColumnResult(name = "sellerId", type = Long.class),
                        @ColumnResult(name = "sellerName", type = String.class),
                        @ColumnResult(name = "productCode", type = String.class),
                        @ColumnResult(name = "productName", type = String.class),
                        @ColumnResult(name = "productImage", type = String.class),
                        @ColumnResult(name = "productUrl", type = String.class),
                        @ColumnResult(name = "productPrice", type = BigDecimal.class),
                        @ColumnResult(name = "productReviewCount", type = Integer.class),
                        @ColumnResult(name = "productRatingScore", type = BigDecimal.class),
                        @ColumnResult(name = "brandId", type = Long.class),
                        @ColumnResult(name = "brandName", type = String.class),
                        @ColumnResult(name = "locationId", type = Integer.class),
                        @ColumnResult(name = "locationName", type = String.class),
                        @ColumnResult(name = "totalSold", type = Long.class),
                        @ColumnResult(name = "totalAmount", type = BigDecimal.class),
                }
        )
)

@Getter
@Setter
@Entity
public class FactSaleFilter {
    @Id
    private Long id;
    private Integer dateId;
    private LocalDate updatedDate;
    private Integer platformId;
    private String platformName;
    private Long categoryId;
    private String categoryName;
    private Long sellerId;
    private String sellerName;
    private String productCode;
    private String productName;
    private String productImage;
    private String productUrl;
    private BigDecimal productPrice;
    private Integer productReviewCount;
    private BigDecimal productRatingScore;
    private Long brandId;
    private String brandName;
    private Integer locationId;
    private String locationName;
    private Long totalSold;
    private BigDecimal totalAmount;
}
