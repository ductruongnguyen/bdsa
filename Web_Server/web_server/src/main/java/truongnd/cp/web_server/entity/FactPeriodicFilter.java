package truongnd.cp.web_server.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDate;


@NamedNativeQuery(
        name = "selectFactPeriodicFilter",
        query = """
                with recent_sales as (select fps.*
                                      from ecommerce.fact_periodic_sales as fps
                                               inner join (select product_code, platform_id, seller_id, max(date_id) as max_date_id
                                                           from ecommerce.fact_periodic_sales
                                                           group by product_code, platform_id, seller_id) as recent
                                                          on fps.product_code = recent.product_code
                                                              and fps.platform_id = recent.platform_id
                                                              and fps.seller_id = recent.seller_id
                                                              and fps.date_id = recent.max_date_id
                                      where fps.units_sold > 0
                                        and fps.total_sales_amount > 0
                                        and (:platform_id is null or fps.platform_id = :platform_id)
                                        and ((fps.report_period = :period)
                                          OR (:period = '30-DAY' AND fps.report_period IN ('14-DAY', '21-DAY', '30-DAY'))
                                          )),
                     filtered_sales as (select recent_sales.*,
                                               ROW_NUMBER() OVER (PARTITION BY product_code, platform_id, seller_id ORDER BY
                                                   CASE
                                                       WHEN report_period = '30-DAY' THEN 3
                                                       WHEN report_period = '21-DAY' THEN 2
                                                       WHEN report_period = '14-DAY' THEN 1
                                                       ELSE 0
                                                       END DESC) AS rn
                                        FROM recent_sales),
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
                select fps.snapshot_id        as id,
                       fps.date_id            as dateId,
                       dd.date                as updatedDate,
                       fps.platform_id        as platformId,
                       dpl.platform_name      as platformName,
                       fps.category_id        as categoryId,
                       dc.category_name       as categoryName,
                       fps.seller_id          as sellerId,
                       ds.seller_name         as sellerName,
                       dp.product_code        as productCode,
                       dp.product_name        as productName,
                       dp.product_image       as productImage,
                       dp.product_url         as productUrl,
                       dp.price               as productPrice,
                       dp.review_count        as productReviewCount,
                       dp.sold                as productAllTimeSold,
                       fps.brand_id           as brandId,
                       db.brand_name          as brandName,
                       fps.location_id        as locationId,
                       dl.location_name       as locationName,
                       fps.units_sold         as sold,
                       fps.total_sales_amount as amount,
                       fps.report_period      as period
                from filtered_sales as fps
                         inner join ecommerce.dim_date dd on fps.date_id = dd.date_id
                         inner join ecommerce.dim_category dc on dc.category_id = fps.category_id
                         inner join (select * from ecommerce.dim_product where flag = true) dp on dp.product_id = fps.product_id
                         inner join ecommerce.dim_location as dl on fps.location_id = dl.location_id
                         inner join ecommerce.dim_seller ds on ds.seller_id = fps.seller_id
                         inner join ecommerce.dim_brand db on db.brand_id = fps.brand_id
                         inner join ecommerce.dim_platform as dpl on fps.platform_id = dpl.platform_id
                where rn = 1
                  and (:category_id is null or exists(select 1 from dim_category_full as dcf where fps.category_id = dcf.category_id))
                  and (:keyword is null or (SELECT COUNT(*)
                                            FROM keywords
                                            WHERE lower(dc.category_name) LIKE '%' || lower(keyword) || '%'
                                               OR lower(dp.product_name) LIKE '%' || lower(keyword) || '%'
                                               OR lower(dp.product_description) LIKE '%' || lower(keyword) || '%') > 0)
                """,
        resultSetMapping = "FactPeriodicFilterMapping"
)

@SqlResultSetMapping(
        name = "FactPeriodicFilterMapping",
        classes = @ConstructorResult(
                targetClass = FactPeriodicFilter.class,
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
                        @ColumnResult(name = "productAllTimeSold", type = Integer.class),
                        @ColumnResult(name = "brandId", type = Long.class),
                        @ColumnResult(name = "brandName", type = String.class),
                        @ColumnResult(name = "locationId", type = Integer.class),
                        @ColumnResult(name = "locationName", type = String.class),
                        @ColumnResult(name = "sold", type = Long.class),
                        @ColumnResult(name = "amount", type = BigDecimal.class),
                        @ColumnResult(name = "period", type = String.class)
                }
        )
)

@Getter
@Setter
@Entity
public class FactPeriodicFilter {
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
    private Integer productAllTimeSold;
    private Long brandId;
    private String brandName;
    private Integer locationId;
    private String locationName;
    private Long sold;
    private BigDecimal amount;
    private String period;
}
