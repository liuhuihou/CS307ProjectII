package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.RecipeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional ;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class RecipeServiceImpl implements RecipeService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public String getNameFromID(long id) {
        String sql = "SELECT Name FROM recipes WHERE RecipeId = ?";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, id);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public RecipeRecord getRecipeById(long recipeId) {
        String sql = "SELECT r.*, u.AuthorName FROM recipes r JOIN users u ON r.AuthorId = u.AuthorId WHERE r.RecipeId = ?";
        try {
            RecipeRecord record = jdbcTemplate.queryForObject(sql, (rs, rowNum) -> mapToRecipeRecord(rs), recipeId);
            if (record != null) {
                record.setRecipeIngredientParts(getIngredients(recipeId));
            }
            return record;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public PageResult<RecipeRecord> searchRecipes(String keyword, String category, Double minRating,
                                                  Integer page, Integer size, String sort) {
        if (page < 1 || size <= 0) {
            throw new IllegalArgumentException("Invalid page or size");
        }

        StringBuilder sqlBuilder = new StringBuilder("SELECT r.*, u.AuthorName FROM recipes r JOIN users u ON r.AuthorId = u.AuthorId WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (keyword != null && !keyword.isEmpty()) {
            sqlBuilder.append(" AND (LOWER(r.Name) LIKE ? OR LOWER(r.Description) LIKE ?)");
            String pattern = "%" + keyword.toLowerCase() + "%";
            params.add(pattern);
            params.add(pattern);
        }

        if (category != null && !category.isEmpty()) {
            sqlBuilder.append(" AND r.RecipeCategory = ?");
            params.add(category);
        }

        if (minRating != null) {
            sqlBuilder.append(" AND r.AggregatedRating >= ?");
            params.add(minRating);
        }

        // Count total
        String countSql = "SELECT COUNT(*) FROM (" + sqlBuilder.toString() + ") as temp";
        Long total = jdbcTemplate.queryForObject(countSql, Long.class, params.toArray());
        if (total == null) total = 0L;

        // Sort
        if (sort != null) {
            switch (sort) {
                case "rating_desc":
                    sqlBuilder.append(" ORDER BY r.AggregatedRating DESC, r.RecipeId DESC");
                    break;
                case "date_desc":
                    sqlBuilder.append(" ORDER BY r.DatePublished DESC, r.RecipeId DESC");
                    break;
                case "calories_asc":
                    sqlBuilder.append(" ORDER BY r.Calories ASC, r.RecipeId DESC");
                    break;
                default:
                    sqlBuilder.append(" ORDER BY r.RecipeId DESC");
                    break;
            }
        } else {
            sqlBuilder.append(" ORDER BY r.RecipeId DESC");
        }

        // Pagination
        sqlBuilder.append(" LIMIT ? OFFSET ?");
        params.add(size);
        params.add((page - 1) * size);

        List<RecipeRecord> records = jdbcTemplate.query(sqlBuilder.toString(), (rs, rowNum) -> mapToRecipeRecord(rs), params.toArray());

        // Batch populate ingredients to avoid N+1 problem
        if (!records.isEmpty()) {
            List<Long> recipeIds = records.stream().map(RecipeRecord::getRecipeId).collect(Collectors.toList());
            String inSql = String.join(",", Collections.nCopies(recipeIds.size(), "?"));
            String ingSql = String.format("SELECT RecipeId, IngredientPart FROM recipe_ingredients WHERE RecipeId IN (%s) ORDER BY RecipeId, IngredientPart ASC", inSql);

            Map<Long, List<String>> ingredientsMap = new HashMap<>();
            jdbcTemplate.query(ingSql, rs -> {
                long rid = rs.getLong("RecipeId");
                String part = rs.getString("IngredientPart");
                ingredientsMap.computeIfAbsent(rid, k -> new ArrayList<>()).add(part);
            }, recipeIds.toArray());

            for (RecipeRecord record : records) {
                List<String> parts = ingredientsMap.get(record.getRecipeId());
                if (parts != null) {
                    parts.sort(String.CASE_INSENSITIVE_ORDER);
                    record.setRecipeIngredientParts(parts.toArray(new String[0]));
                } else {
                    record.setRecipeIngredientParts(new String[0]);
                }
            }
        }

        return new PageResult<>(records, page, size, total);
    }

    @Override
    @Transactional
    public long createRecipe(RecipeRecord dto, AuthInfo auth) {
        validateUser(auth);
        if (dto.getName() == null || dto.getName().isEmpty()) {
            throw new IllegalArgumentException("Recipe name cannot be empty");
        }

        String sql = "INSERT INTO recipes (Name, AuthorId, CookTime, PrepTime, TotalTime, DatePublished, " +
                "Description, RecipeCategory, AggregatedRating, ReviewCount, Calories, FatContent, SaturatedFatContent, " +
                "CholesterolContent, SodiumContent, CarbohydrateContent, FiberContent, SugarContent, ProteinContent, " +
                "RecipeServings, RecipeYield) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING RecipeId";

        // Calculate TotalTime if possible
        String totalTime = dto.getTotalTime();
        if (dto.getCookTime() != null && dto.getPrepTime() != null) {
            try {
                Duration cook = Duration.parse(dto.getCookTime());
                Duration prep = Duration.parse(dto.getPrepTime());
                totalTime = cook.plus(prep).toString();
            } catch (Exception ignored) {}
        }

        Long newId = jdbcTemplate.queryForObject(sql, Long.class,
                dto.getName(),
                auth.getAuthorId(),
                dto.getCookTime(),
                dto.getPrepTime(),
                totalTime,
                new java.sql.Timestamp(System.currentTimeMillis()), // DatePublished
                dto.getDescription(),
                dto.getRecipeCategory(),
                0.0f, // AggregatedRating
                0, // ReviewCount
                dto.getCalories(),
                dto.getFatContent(),
                dto.getSaturatedFatContent(),
                dto.getCholesterolContent(),
                dto.getSodiumContent(),
                dto.getCarbohydrateContent(),
                dto.getFiberContent(),
                dto.getSugarContent(),
                dto.getProteinContent(),
                String.valueOf(dto.getRecipeServings()),
                dto.getRecipeYield()
        );

        if (newId != null && dto.getRecipeIngredientParts() != null) {
            String ingSql = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) VALUES (?, ?)";
            for (String part : dto.getRecipeIngredientParts()) {
                jdbcTemplate.update(ingSql, newId, part);
            }
        }

        return newId != null ? newId : 0L;
    }

    @Override
    @Transactional
    public void deleteRecipe(long recipeId, AuthInfo auth) {
        validateUser(auth);
        checkOwnership(recipeId, auth.getAuthorId());

        // Delete dependencies
        // 1. Review Likes
        jdbcTemplate.update("DELETE FROM review_likes WHERE ReviewId IN (SELECT ReviewId FROM reviews WHERE RecipeId = ?)", recipeId);
        // 2. Reviews
        jdbcTemplate.update("DELETE FROM reviews WHERE RecipeId = ?", recipeId);
        // 3. Ingredients
        jdbcTemplate.update("DELETE FROM recipe_ingredients WHERE RecipeId = ?", recipeId);
        // 4. Recipe
        jdbcTemplate.update("DELETE FROM recipes WHERE RecipeId = ?", recipeId);
    }

    @Override
    @Transactional
    public void updateTimes(AuthInfo auth, long recipeId, String cookTimeIso, String prepTimeIso) {
        validateUser(auth);
        checkOwnership(recipeId, auth.getAuthorId());

        if (cookTimeIso == null && prepTimeIso == null) {
            return;
        }

        // Fetch current times if one is null
        String currentCookTime = null;
        String currentPrepTime = null;

        try {
            Map<String, Object> times = jdbcTemplate.queryForMap("SELECT CookTime, PrepTime FROM recipes WHERE RecipeId = ?", recipeId);
            currentCookTime = (String) times.get("CookTime");
            currentPrepTime = (String) times.get("PrepTime");
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Recipe not found");
        }

        String newCookTime = cookTimeIso != null ? cookTimeIso : currentCookTime;
        String newPrepTime = prepTimeIso != null ? prepTimeIso : currentPrepTime;

        Duration cook = Duration.ZERO;
        Duration prep = Duration.ZERO;

        try {
            if (newCookTime != null && !newCookTime.isEmpty()) cook = Duration.parse(newCookTime);
            if (newPrepTime != null && !newPrepTime.isEmpty()) prep = Duration.parse(newPrepTime);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid duration format");
        }

        if (cook.isNegative() || prep.isNegative()) {
            throw new IllegalArgumentException("Duration cannot be negative");
        }

        String totalTime = cook.plus(prep).toString();

        jdbcTemplate.update("UPDATE recipes SET CookTime = ?, PrepTime = ?, TotalTime = ? WHERE RecipeId = ?",
                newCookTime, newPrepTime, totalTime, recipeId);
    }

    @Override
    public Map<String, Object> getClosestCaloriePair() {
        // Optimized using window functions to avoid O(N^2) cross join
        String sql = "WITH SortedRecipes AS (" +
                "    SELECT RecipeId, Calories, " +
                "           LAG(Calories) OVER (ORDER BY Calories ASC, RecipeId ASC) as PrevCalories, " +
                "           LAG(RecipeId) OVER (ORDER BY Calories ASC, RecipeId ASC) as PrevRecipeId " +
                "    FROM recipes " +
                "    WHERE Calories IS NOT NULL " +
                ") " +
                "SELECT " +
                "    CASE WHEN RecipeId < PrevRecipeId THEN RecipeId ELSE PrevRecipeId END as RecipeA, " +
                "    CASE WHEN RecipeId < PrevRecipeId THEN PrevRecipeId ELSE RecipeId END as RecipeB, " +
                "    CASE WHEN RecipeId < PrevRecipeId THEN Calories ELSE PrevCalories END as CaloriesA, " +
                "    CASE WHEN RecipeId < PrevRecipeId THEN PrevCalories ELSE Calories END as CaloriesB, " +
                "    ABS(CAST(Calories AS NUMERIC) - CAST(PrevCalories AS NUMERIC)) as diff " +
                "FROM SortedRecipes " +
                "WHERE PrevCalories IS NOT NULL " +
                "ORDER BY diff ASC, RecipeA ASC, RecipeB ASC " +
                "LIMIT 1";

        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                Map<String, Object> map = new HashMap<>();
                map.put("RecipeA", rs.getLong("RecipeA"));
                map.put("RecipeB", rs.getLong("RecipeB"));
                map.put("CaloriesA", rs.getDouble("CaloriesA"));
                map.put("CaloriesB", rs.getDouble("CaloriesB"));
                map.put("Difference", rs.getDouble("diff"));
                return map;
            });
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public List<Map<String, Object>> getTop3MostComplexRecipesByIngredients() {
        String sql = "SELECT r.RecipeId, r.Name, COUNT(ri.IngredientPart) as cnt " +
                "FROM recipes r JOIN recipe_ingredients ri ON r.RecipeId = ri.RecipeId " +
                "GROUP BY r.RecipeId, r.Name " +
                "ORDER BY cnt DESC, r.RecipeId ASC " +
                "LIMIT 3";

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> map = new HashMap<>();
            map.put("RecipeId", rs.getLong("RecipeId"));
            map.put("Name", rs.getString("Name"));
            map.put("IngredientCount", rs.getInt("cnt"));
            return map;
        });
    }

    private void validateUser(AuthInfo auth) {
        if (auth == null) {
            throw new SecurityException("Auth info is null");
        }
        String sql = "SELECT IsDeleted FROM users WHERE AuthorId = ?";
        try {
            Boolean isDeleted = jdbcTemplate.queryForObject(sql, Boolean.class, auth.getAuthorId());
            if (isDeleted == null || isDeleted) {
                throw new SecurityException("User is deleted or does not exist");
            }
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("User does not exist");
        }
    }

    private void checkOwnership(long recipeId, long authorId) {
        String sql = "SELECT AuthorId FROM recipes WHERE RecipeId = ?";
        try {
            Long ownerId = jdbcTemplate.queryForObject(sql, Long.class, recipeId);
            if (ownerId == null || ownerId != authorId) {
                throw new SecurityException("User is not the owner of the recipe");
            }
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("Recipe not found");
        }
    }

    private String[] getIngredients(long recipeId) {
        String sql = "SELECT IngredientPart FROM recipe_ingredients WHERE RecipeId = ? ORDER BY IngredientPart ASC";
        List<String> ingredients = jdbcTemplate.queryForList(sql, String.class, recipeId);
        ingredients.sort(String.CASE_INSENSITIVE_ORDER);
        return ingredients.toArray(new String[0]);
    }

    private RecipeRecord mapToRecipeRecord(ResultSet rs) throws SQLException {
        RecipeRecord record = new RecipeRecord();
        record.setRecipeId(rs.getLong("RecipeId"));
        record.setName(rs.getString("Name"));
        record.setAuthorId(rs.getLong("AuthorId"));
        record.setAuthorName(rs.getString("AuthorName"));
        record.setCookTime(rs.getString("CookTime"));
        record.setPrepTime(rs.getString("PrepTime"));
        record.setTotalTime(rs.getString("TotalTime"));
        record.setDatePublished(rs.getTimestamp("DatePublished"));
        record.setDescription(rs.getString("Description"));
        record.setRecipeCategory(rs.getString("RecipeCategory"));
        record.setAggregatedRating(rs.getFloat("AggregatedRating"));
        record.setReviewCount(rs.getInt("ReviewCount"));
        record.setCalories(rs.getFloat("Calories"));
        record.setFatContent(rs.getFloat("FatContent"));
        record.setSaturatedFatContent(rs.getFloat("SaturatedFatContent"));
        record.setCholesterolContent(rs.getFloat("CholesterolContent"));
        record.setSodiumContent(rs.getFloat("SodiumContent"));
        record.setCarbohydrateContent(rs.getFloat("CarbohydrateContent"));
        record.setFiberContent(rs.getFloat("FiberContent"));
        record.setSugarContent(rs.getFloat("SugarContent"));
        record.setProteinContent(rs.getFloat("ProteinContent"));
        try {
            String servings = rs.getString("RecipeServings");
            if (servings != null && !servings.isEmpty() && !servings.equals("null")) {
                record.setRecipeServings(Integer.parseInt(servings));
            }
        } catch (NumberFormatException e) {
            // ignore
        }
        record.setRecipeYield(rs.getString("RecipeYield"));
        return record;
    }
}