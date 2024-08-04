package truongnd.cp.web_server.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import truongnd.cp.web_server.entity.DimPlatform;

@Repository
public interface DimPlatformRepository extends JpaRepository<DimPlatform, Integer> {
}
