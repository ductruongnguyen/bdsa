package truongnd.cp.web_server.repository;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import truongnd.cp.web_server.entity.FactPeriodicFilter;

import java.util.List;
import java.util.Optional;

@Repository
public interface FactPeriodicSalesFilterRepository extends CrudRepository<FactPeriodicFilter, Long> {

    @Query(name = "selectFactPeriodicFilter", nativeQuery = true)
    Optional<List<FactPeriodicFilter>> findFactPeriodicFilter(
            @Param("platform_id") Integer platformId,
            @Param("category_id") Long categoryId,
            @Param("period") String period,
            @Param("keyword") String keyword);
}
