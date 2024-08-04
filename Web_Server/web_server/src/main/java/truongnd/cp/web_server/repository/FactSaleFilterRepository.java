package truongnd.cp.web_server.repository;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import truongnd.cp.web_server.entity.FactSaleFilter;

import java.util.List;
import java.util.Optional;

@Repository
public interface FactSaleFilterRepository extends CrudRepository<FactSaleFilter, Long> {

    @Query(name = "selectFactSaleFilter", nativeQuery = true)
    Optional<List<FactSaleFilter>> findFactSaleFilter(
            @Param("platform_id") Integer platformId,
            @Param("category_id") Long categoryId,
            @Param("keyword") String keyword);
}
