package truongnd.cp.web_server.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "dim_platform", schema = "ecommerce")
public class DimPlatform {
    @Id
    @Column(name = "platform_id", nullable = false)
    private Integer platformId;

    @Size(max = 255)
    @NotNull
    @Column(name = "platform_name", nullable = false)
    private String platformName;
}