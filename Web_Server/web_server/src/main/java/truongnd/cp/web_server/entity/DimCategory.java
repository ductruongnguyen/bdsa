package truongnd.cp.web_server.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "dim_category", schema = "ecommerce")
public class DimCategory {
    @Id
    @Column(name = "category_id", nullable = false)
    private Long categoryId;

    @Column(name = "category_code", nullable = false)
    private Long categoryCode;

    @Column(name = "platform_id", nullable = false)
    private Integer platformId;

    @Column(name = "category_name")
    private String categoryName;

    @Column(name = "category_url")
    private String categoryUrl;

    @Column(name = "parent_id")
    private Long parentId;
}