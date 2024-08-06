package truongnd.cp.web_server.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import truongnd.cp.web_server.entity.User;

@Repository
public interface UsersRepository extends JpaRepository<User, Long> {
    User findByEmailAndProvider(String email, String provider);
}
