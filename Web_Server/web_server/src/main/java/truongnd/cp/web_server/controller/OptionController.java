package truongnd.cp.web_server.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import truongnd.cp.web_server.entity.DimCategory;
import truongnd.cp.web_server.repository.DimCategoryRepository;

import java.util.List;
import java.util.Objects;

@RestController
@RequiredArgsConstructor
public class OptionController {

    private final DimCategoryRepository categoryRepository;

    @GetMapping("/categories/{platformId}")
    public List<DimCategory> getCategoriesByPlatform(@PathVariable Integer platformId) {
        return categoryRepository.findAll().stream()
                .filter(category -> Objects.equals(category.getPlatformId(), platformId) &&
                        category.getParentId() == null)
                .sorted((category1, category2) ->
                        category2.getCategoryId().compareTo(category1.getCategoryId()))
                .toList();
    }
}
