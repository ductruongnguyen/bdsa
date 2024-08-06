package truongnd.cp.web_server.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import truongnd.cp.web_server.dto.CustomOAuth2User;
import truongnd.cp.web_server.entity.User;
import truongnd.cp.web_server.repository.UsersRepository;

@Service
public class UserService {
    @Autowired
    private UsersRepository usersRepository;

    public void processOAuthPostLogin(CustomOAuth2User oAuth2User) {
        String name = oAuth2User.getName();
        String email = oAuth2User.getEmail();
        String provider = oAuth2User.getProvider();

        System.out.println("Logged in user infor: " + name + " " + email + " " + provider);

        User user = usersRepository.findByEmailAndProvider(email, provider);
        if (user == null) {
            user = new User();
            user.setEmail(email);
            user.setName(name);
            user.setProvider(provider);
            usersRepository.save(user);
        }
    }
}
