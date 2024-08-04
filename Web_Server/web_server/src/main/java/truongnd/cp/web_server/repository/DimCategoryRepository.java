package truongnd.cp.web_server.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import truongnd.cp.web_server.entity.DimCategory;

@Repository
public interface DimCategoryRepository extends JpaRepository<DimCategory, Long> {
}
