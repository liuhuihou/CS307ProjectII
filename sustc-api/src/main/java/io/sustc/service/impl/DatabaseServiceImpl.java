// java
package io.sustc.service.impl;

import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public List<Integer> getGroupMembers() {
        return Arrays.asList(12412103, 12411103);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void importData(List<ReviewRecord> reviewRecords,
                           List<UserRecord> userRecords,
                           List<RecipeRecord> recipeRecords) {

        Objects.requireNonNull(reviewRecords, "reviewRecords cannot be null");
        Objects.requireNonNull(userRecords, "userRecords cannot be null");
        Objects.requireNonNull(recipeRecords, "recipeRecords cannot be null");

        log.info("Starting data import...");
        createTables();
        log.info("Tables created.");
        truncateTables();
        log.info("Tables truncated.");

        log.info("Inserting {} users...", userRecords.size());
        batchInsertUsers(userRecords);
        log.info("Inserting user follows...");
        batchInsertUserFollows(userRecords);

        log.info("Inserting {} recipes...", recipeRecords.size());
        batchInsertRecipes(recipeRecords);
        log.info("Inserting recipe ingredients...");
        batchInsertRecipeIngredients(recipeRecords);

        log.info("Inserting {} reviews...", reviewRecords.size());
        batchInsertReviews(reviewRecords);

        // 关键：导入历史 reviewid 后推进 Identity 序列到 MAX(reviewid)
        resetReviewIdSequence();

        log.info("Inserting review likes...");
        batchInsertReviewLikes(reviewRecords);

        log.info("Imported {} users, {} recipes and {} reviews.",
                userRecords.size(), recipeRecords.size(), reviewRecords.size());
    }

    private void batchInsertUsers(List<UserRecord> users) {
        if (users.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO users (AuthorId, AuthorName, Gender, Age, Followers, Following, Password, IsDeleted) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (AuthorId) DO UPDATE SET " +
                "AuthorName = EXCLUDED.AuthorName, Gender = EXCLUDED.Gender, Age = EXCLUDED.Age, " +
                "Followers = EXCLUDED.Followers, Following = EXCLUDED.Following, Password = EXCLUDED.Password, " +
                "IsDeleted = EXCLUDED.IsDeleted";

        int batchSize = 1000;
        for (int i = 0; i < users.size(); i += batchSize) {
            List<UserRecord> batch = users.subList(i, Math.min(i + batchSize, users.size()));
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    UserRecord user = batch.get(i);
                    ps.setLong(1, user.getAuthorId());
                    ps.setString(2, user.getAuthorName());
                    ps.setString(3, user.getGender());
                    ps.setObject(4, user.getAge());
                    ps.setObject(5, user.getFollowers());
                    ps.setObject(6, user.getFollowing());
                    ps.setString(7, user.getPassword());
                    ps.setBoolean(8, user.isDeleted());
                }

                @Override
                public int getBatchSize() {
                    return batch.size();
                }
            });
        }
    }

    private void batchInsertUserFollows(List<UserRecord> users) {
        List<Object[]> relations = new ArrayList<>();
        for (UserRecord user : users) {
            long followerId = user.getAuthorId();
            long[] followingUsers = user.getFollowingUsers();
            if (followingUsers == null || followingUsers.length == 0) {
                continue;
            }
            for (long followeeId : followingUsers) {
                if (followeeId <= 0 || followeeId == followerId) {
                    continue;
                }
                relations.add(new Object[]{followerId, followeeId});
            }
        }
        if (relations.isEmpty()) {
            return;
        }

        int batchSize = 1000;
        for (int i = 0; i < relations.size(); i += batchSize) {
            List<Object[]> batch = relations.subList(i, Math.min(i + batchSize, relations.size()));
            jdbcTemplate.batchUpdate(
                    "INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?) ON CONFLICT DO NOTHING",
                    batch);
        }
    }

    private void batchInsertRecipes(List<RecipeRecord> recipes) {
        if (recipes.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO recipes (RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, DatePublished, " +
                "Description, RecipeCategory, AggregatedRating, ReviewCount, Calories, FatContent, SaturatedFatContent, " +
                "CholesterolContent, SodiumContent, CarbohydrateContent, FiberContent, SugarContent, ProteinContent, " +
                "RecipeServings, RecipeYield) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
                "ON CONFLICT (RecipeId) DO UPDATE SET " +
                "Name = EXCLUDED.Name, AuthorId = EXCLUDED.AuthorId, CookTime = EXCLUDED.CookTime, " +
                "PrepTime = EXCLUDED.PrepTime, TotalTime = EXCLUDED.TotalTime, DatePublished = EXCLUDED.DatePublished, " +
                "Description = EXCLUDED.Description, RecipeCategory = EXCLUDED.RecipeCategory, " +
                "AggregatedRating = EXCLUDED.AggregatedRating, ReviewCount = EXCLUDED.ReviewCount, " +
                "Calories = EXCLUDED.Calories, FatContent = EXCLUDED.FatContent, " +
                "SaturatedFatContent = EXCLUDED.SaturatedFatContent, CholesterolContent = EXCLUDED.CholesterolContent, " +
                "SodiumContent = EXCLUDED.SodiumContent, CarbohydrateContent = EXCLUDED.CarbohydrateContent, " +
                "FiberContent = EXCLUDED.FiberContent, SugarContent = EXCLUDED.SugarContent, " +
                "ProteinContent = EXCLUDED.ProteinContent, RecipeServings = EXCLUDED.RecipeServings, " +
                "RecipeYield = EXCLUDED.RecipeYield";

        int batchSize = 1000;
        for (int i = 0; i < recipes.size(); i += batchSize) {
            List<RecipeRecord> batch = recipes.subList(i, Math.min(i + batchSize, recipes.size()));
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    RecipeRecord recipe = batch.get(i);
                    ps.setLong(1, recipe.getRecipeId());
                    ps.setString(2, recipe.getName());
                    ps.setLong(3, recipe.getAuthorId());
                    ps.setString(4, recipe.getCookTime());
                    ps.setString(5, recipe.getPrepTime());
                    ps.setString(6, recipe.getTotalTime());
                    ps.setTimestamp(7, recipe.getDatePublished());
                    ps.setString(8, recipe.getDescription());
                    ps.setString(9, recipe.getRecipeCategory());
                    ps.setObject(10, recipe.getAggregatedRating());
                    ps.setObject(11, recipe.getReviewCount());
                    ps.setObject(12, recipe.getCalories());
                    ps.setObject(13, recipe.getFatContent());
                    ps.setObject(14, recipe.getSaturatedFatContent());
                    ps.setObject(15, recipe.getCholesterolContent());
                    ps.setObject(16, recipe.getSodiumContent());
                    ps.setObject(17, recipe.getCarbohydrateContent());
                    ps.setObject(18, recipe.getFiberContent());
                    ps.setObject(19, recipe.getSugarContent());
                    ps.setObject(20, recipe.getProteinContent());
                    ps.setString(21, String.valueOf(recipe.getRecipeServings()));
                    ps.setString(22, recipe.getRecipeYield());
                }

                @Override
                public int getBatchSize() {
                    return batch.size();
                }
            });
        }
    }

    private void batchInsertRecipeIngredients(List<RecipeRecord> recipes) {
        List<Object[]> ingredients = new ArrayList<>();
        for (RecipeRecord recipe : recipes) {
            String[] parts = recipe.getRecipeIngredientParts();
            if (parts == null || parts.length == 0) {
                continue;
            }
            for (String part : parts) {
                if (part == null || part.isBlank()) {
                    continue;
                }
                ingredients.add(new Object[]{recipe.getRecipeId(), part});
            }
        }
        if (ingredients.isEmpty()) {
            return;
        }

        int batchSize = 1000;
        for (int i = 0; i < ingredients.size(); i += batchSize) {
            List<Object[]> batch = ingredients.subList(i, Math.min(i + batchSize, ingredients.size()));
            jdbcTemplate.batchUpdate(
                    "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) VALUES (?, ?) ON CONFLICT DO NOTHING",
                    batch);
        }
    }

    private void batchInsertReviews(List<ReviewRecord> reviews) {
        if (reviews.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (ReviewId) DO UPDATE SET " +
                "RecipeId = EXCLUDED.RecipeId, AuthorId = EXCLUDED.AuthorId, Rating = EXCLUDED.Rating, " +
                "Review = EXCLUDED.Review, DateSubmitted = EXCLUDED.DateSubmitted, DateModified = EXCLUDED.DateModified";

        int batchSize = 1000;
        for (int i = 0; i < reviews.size(); i += batchSize) {
            List<ReviewRecord> batch = reviews.subList(i, Math.min(i + batchSize, reviews.size()));
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ReviewRecord review = batch.get(i);
                    ps.setLong(1, review.getReviewId());
                    ps.setLong(2, review.getRecipeId());
                    ps.setLong(3, review.getAuthorId());
                    ps.setObject(4, review.getRating());
                    ps.setString(5, review.getReview());
                    ps.setTimestamp(6, review.getDateSubmitted());
                    ps.setTimestamp(7, review.getDateModified());
                }

                @Override
                public int getBatchSize() {
                    return batch.size();
                }
            });
        }
    }

    private void batchInsertReviewLikes(List<ReviewRecord> reviews) {
        List<Object[]> likes = new ArrayList<>();
        for (ReviewRecord review : reviews) {
            long[] likedUsers = review.getLikes();
            if (likedUsers == null || likedUsers.length == 0) {
                continue;
            }
            for (long userId : likedUsers) {
                if (userId <= 0) {
                    continue;
                }
                likes.add(new Object[]{review.getReviewId(), userId});
            }
        }
        if (likes.isEmpty()) {
            return;
        }

        int batchSize = 1000;
        for (int i = 0; i < likes.size(); i += batchSize) {
            List<Object[]> batch = likes.subList(i, Math.min(i + batchSize, likes.size()));
            jdbcTemplate.batchUpdate(
                    "INSERT INTO review_likes (ReviewId, AuthorId) VALUES (?, ?) ON CONFLICT DO NOTHING",
                    batch);
        }
    }

    private void truncateTables() {
        jdbcTemplate.execute("TRUNCATE TABLE review_likes, reviews, recipe_ingredients, recipes, user_follows, users RESTART IDENTITY CASCADE");
    }

    private void createTables() {
        String[] createTableSQLs = {
                "CREATE TABLE IF NOT EXISTS users (" +
                        "    AuthorId BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "    AuthorName VARCHAR(255) NOT NULL, " +
                        "    Gender VARCHAR(10) CHECK (Gender IN ('Male', 'Female')), " +
                        "    Age INTEGER CHECK (Age > 0), " +
                        "    Followers INTEGER DEFAULT 0 CHECK (Followers >= 0), " +
                        "    Following INTEGER DEFAULT 0 CHECK (Following >= 0), " +
                        "    Password VARCHAR(255), " +
                        "    IsDeleted BOOLEAN DEFAULT FALSE" +
                        ")",
                "CREATE TABLE IF NOT EXISTS recipes (" +
                        "    RecipeId BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "    Name VARCHAR(500) NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    CookTime VARCHAR(50), " +
                        "    PrepTime VARCHAR(50), " +
                        "    TotalTime VARCHAR(50), " +
                        "    DatePublished TIMESTAMP, " +
                        "    Description TEXT, " +
                        "    RecipeCategory VARCHAR(255), " +
                        "    AggregatedRating DECIMAL(3,2) CHECK (AggregatedRating >= 0 AND AggregatedRating <= 5), " +
                        "    ReviewCount INTEGER DEFAULT 0 CHECK (ReviewCount >= 0), " +
                        "    Calories DECIMAL(10,2), " +
                        "    FatContent DECIMAL(10,2), " +
                        "    SaturatedFatContent DECIMAL(10,2), " +
                        "    CholesterolContent DECIMAL(10,2), " +
                        "    SodiumContent DECIMAL(10,2), " +
                        "    CarbohydrateContent DECIMAL(10,2), " +
                        "    FiberContent DECIMAL(10,2), " +
                        "    SugarContent DECIMAL(10,2), " +
                        "    ProteinContent DECIMAL(10,2), " +
                        "    RecipeServings VARCHAR(100), " +
                        "    RecipeYield VARCHAR(100), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",
                // 关键：ReviewId 使用 Identity，支持 DEFAULT 自增与显式插入
                "CREATE TABLE IF NOT EXISTS reviews (" +
                        "    ReviewId BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "    RecipeId BIGINT NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    Rating INTEGER, " +
                        "    Review TEXT, " +
                        "    DateSubmitted TIMESTAMP NOT NULL DEFAULT now(), " +
                        "    DateModified TIMESTAMP NOT NULL DEFAULT now(), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",
                "CREATE TABLE IF NOT EXISTS recipe_ingredients (" +
                        "    RecipeId BIGINT, " +
                        "    IngredientPart VARCHAR(500), " +
                        "    PRIMARY KEY (RecipeId, IngredientPart), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId)" +
                        ")",
                "CREATE TABLE IF NOT EXISTS review_likes (" +
                        "    ReviewId BIGINT, " +
                        "    AuthorId BIGINT, " +
                        "    PRIMARY KEY (ReviewId, AuthorId), " +
                        "    FOREIGN KEY (ReviewId) REFERENCES reviews(ReviewId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",
                "CREATE TABLE IF NOT EXISTS user_follows (" +
                        "    FollowerId BIGINT, " +
                        "    FollowingId BIGINT, " +
                        "    PRIMARY KEY (FollowerId, FollowingId), " +
                        "    FOREIGN KEY (FollowerId) REFERENCES users(AuthorId), " +
                        "    FOREIGN KEY (FollowingId) REFERENCES users(AuthorId), " +
                        "    CHECK (FollowerId != FollowingId)" +
                        ")"
        };
        for (String sql : createTableSQLs) {
            jdbcTemplate.execute(sql);
        }
    }

    private void resetUserIdSequence() {
        jdbcTemplate.execute(
                "SELECT setval(" +
                        "pg_get_serial_sequence('public.users','authorid'), " +
                        "COALESCE((SELECT MAX(authorid) FROM public.users), 0), " +
                        "true" +
                        ")"
        );
    }

    private void resetRecipeIdSequence() {
        jdbcTemplate.execute(
                "SELECT setval(" +
                        "pg_get_serial_sequence('public.recipes','recipeid'), " +
                        "COALESCE((SELECT MAX(recipeid) FROM public.recipes), 0), " +
                        "true" +
                        ")"
        );
    }

    // 将 reviews 的 Identity 序列推进到当前最大主键，防止后续 DEFAULT 产生冲突
    private void resetReviewIdSequence() {
        jdbcTemplate.execute(
                "SELECT setval(" +
                        "pg_get_serial_sequence('public.reviews','reviewid'), " +
                        "COALESCE((SELECT MAX(reviewid) FROM public.reviews), 0), " +
                        "true" +
                        ")"
        );
    }

    @Override
    public void drop() {
        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'DROP TABLE IF EXISTS ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
