package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.ReviewService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ReviewServiceImpl implements ReviewService {

    @Autowired
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    private JdbcTemplate jdbcTemplate;

    @Autowired
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    private DataSource dataSource;

    @Override
    @Transactional
    public long addReview(AuthInfo auth, long recipeId, int rating, String review) {
        validateActiveUser(auth);
        ensureRecipeExists(recipeId);
        if (rating < 1 || rating > 5) throw new IllegalArgumentException("rating must be in [1,5]");

        Timestamp now = new Timestamp(System.currentTimeMillis());
        Long reviewId = jdbcTemplate.queryForObject(
                "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                        "VALUES (DEFAULT, ?, ?, ?, ?, ?, ?) RETURNING ReviewId",
                Long.class, recipeId, auth.getAuthorId(), rating, review, now, now);

        refreshRecipeAggregatedRating(recipeId);
        return reviewId == null ? 0L : reviewId;
    }

    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        validateActiveUser(auth);
        ensureRecipeExists(recipeId);
        ensureReviewBelongsToRecipe(reviewId, recipeId);
        if (rating < 1 || rating > 5) throw new IllegalArgumentException("rating must be in [1,5]");

        Long authorId = jdbcTemplate.queryForObject("SELECT AuthorId FROM reviews WHERE ReviewId = ?", Long.class, reviewId);
        if (authorId == null || !authorId.equals(auth.getAuthorId())) throw new SecurityException("not review author");

        Timestamp now = new Timestamp(System.currentTimeMillis());
        jdbcTemplate.update("UPDATE reviews SET Rating = ?, Review = ?, DateModified = ? WHERE ReviewId = ?",
                rating, review, now, reviewId);

        refreshRecipeAggregatedRating(recipeId);
    }

    @Override
    @Transactional
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {
        validateActiveUser(auth);
        ensureRecipeExists(recipeId);
        ensureReviewBelongsToRecipe(reviewId, recipeId);

        Long authorId = jdbcTemplate.queryForObject("SELECT AuthorId FROM reviews WHERE ReviewId = ?", Long.class, reviewId);
        if (authorId == null || !authorId.equals(auth.getAuthorId())) throw new SecurityException("not review author");

        jdbcTemplate.update("DELETE FROM review_likes WHERE ReviewId = ?", reviewId);
        jdbcTemplate.update("DELETE FROM reviews WHERE ReviewId = ?", reviewId);

        refreshRecipeAggregatedRating(recipeId);
        resetPkSequenceToNext("reviews");
    }

    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        validateActiveUser(auth);
        Long reviewAuthor;
        try {
            reviewAuthor = jdbcTemplate.queryForObject("SELECT AuthorId FROM reviews WHERE ReviewId = ?", Long.class, reviewId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("review not exists");
        }

        if (reviewAuthor.equals(auth.getAuthorId())) throw new SecurityException("cannot like own review");

        jdbcTemplate.update("INSERT INTO review_likes (ReviewId, AuthorId) VALUES (?, ?) ON CONFLICT DO NOTHING",
                reviewId, auth.getAuthorId());

        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM review_likes WHERE ReviewId = ?", Long.class, reviewId);
    }

    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        validateActiveUser(auth);
        Integer exists = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reviews WHERE ReviewId = ?", Integer.class, reviewId);
        if (exists == null || exists == 0) throw new IllegalArgumentException("review not exists");

        jdbcTemplate.update("DELETE FROM review_likes WHERE ReviewId = ? AND AuthorId = ?", reviewId, auth.getAuthorId());
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM review_likes WHERE ReviewId = ?", Long.class, reviewId);
    }

    @Override
    @Transactional(readOnly = true)
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        if (page < 1 || size <= 0) throw new IllegalArgumentException("invalid page/size");
        ensureRecipeExists(recipeId);

        String normalized = normalizeSort(sort);

        String orderBy;
        if ("likes_desc".equals(normalized)) {
            orderBy = "x.likes_count DESC, r.DateModified DESC, r.ReviewId ASC";
        } else { // date_desc 或未知/空白：默认 date_desc
            orderBy = "r.DateModified DESC, r.ReviewId ASC";
        }

        int offset = (page - 1) * size;

        String sql =
                "SELECT r.*, u.AuthorName, x.likes_count " +
                        "FROM reviews r " +
                        "JOIN users u ON r.AuthorId = u.AuthorId " +
                        "JOIN ( " +
                        "  SELECT rv.ReviewId, COUNT(rl.AuthorId) AS likes_count " +
                        "  FROM reviews rv " +
                        "  LEFT JOIN review_likes rl ON rl.ReviewId = rv.ReviewId " +
                        "  WHERE rv.RecipeId = ? " +
                        "  GROUP BY rv.ReviewId " +
                        ") x ON x.ReviewId = r.ReviewId " +
                        "WHERE r.RecipeId = ? " +
                        "ORDER BY " + orderBy + " " +
                        "LIMIT ? OFFSET ?";

        List<ReviewRecord> items = jdbcTemplate.query(sql, (rs, rowNum) -> {
            long reviewId = rs.getLong("ReviewId");

            List<Long> likerIds = jdbcTemplate.queryForList(
                    "SELECT AuthorId FROM review_likes WHERE ReviewId = ? ORDER BY AuthorId ASC",
                    Long.class,
                    reviewId
            );

            return ReviewRecord.builder()
                    .reviewId(reviewId)
                    .recipeId(rs.getLong("RecipeId"))
                    .authorId(rs.getLong("AuthorId"))
                    .authorName(rs.getString("AuthorName"))
                    .rating(rs.getInt("Rating"))
                    .review(rs.getString("Review"))
                    .dateSubmitted(rs.getTimestamp("DateSubmitted"))
                    .dateModified(rs.getTimestamp("DateModified"))
                    .likes(likerIds.stream().mapToLong(Long::longValue).toArray())
                    .build();
        }, recipeId, recipeId, size, offset);

        Long total = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reviews WHERE RecipeId = ?",
                Long.class,
                recipeId
        );

        return PageResult.<ReviewRecord>builder()
                .items(items)
                .page(page)
                .size(size)
                .total(total == null ? 0 : total)
                .build();
    }

    private String normalizeSort(String sort) {
        String s = (sort == null ? "" : sort.trim().toLowerCase());

        if (s.isBlank()) return "date_desc";

        // 兼容常见误输入
        if ("data_desc".equals(s)) return "date_desc";
        if ("like_desc".equals(s)) return "likes_desc";
        if ("likes_descs".equals(s)) return "likes_desc";

        if ("date_desc".equals(s)) return "date_desc";
        if ("likes_desc".equals(s)) return "likes_desc";

        // 未知 sort：默认 date_desc
        return "date_desc";
    }

    @Override
    @Transactional
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        ensureRecipeExists(recipeId);

        Map<String, Object> agg = jdbcTemplate.queryForMap(
                "SELECT ROUND(AVG(Rating)::numeric, 2) AS avg_rating, COUNT(*) AS cnt FROM reviews WHERE RecipeId = ?",
                recipeId);

        Number cnt = (Number) agg.get("cnt");
        Number avg = (Number) agg.get("avg_rating");

        if (cnt == null || cnt.longValue() == 0L) {
            jdbcTemplate.update("UPDATE recipes SET AggregatedRating = NULL, ReviewCount = 0 WHERE RecipeId = ?", recipeId);
        } else {
            jdbcTemplate.update("UPDATE recipes SET AggregatedRating = ?, ReviewCount = ? WHERE RecipeId = ?",
                    avg, cnt.longValue(), recipeId);
        }

        return jdbcTemplate.queryForObject(
                "SELECT RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, DatePublished, Description, " +
                        "       RecipeCategory, AggregatedRating, ReviewCount, Calories, FatContent, SaturatedFatContent, " +
                        "       CholesterolContent, SodiumContent, CarbohydrateContent, FiberContent, SugarContent, " +
                        "       ProteinContent, RecipeServings, RecipeYield " +
                        "FROM recipes WHERE RecipeId = ?",
                (rs, i) -> io.sustc.dto.RecipeRecord.builder()
                        .RecipeId(rs.getLong("RecipeId"))
                        .name(rs.getString("Name"))
                        .authorId(rs.getLong("AuthorId"))
                        .cookTime(rs.getString("CookTime"))
                        .prepTime(rs.getString("PrepTime"))
                        .totalTime(rs.getString("TotalTime"))
                        .datePublished(rs.getTimestamp("DatePublished"))
                        .description(rs.getString("Description"))
                        .recipeCategory(rs.getString("RecipeCategory"))
                        .aggregatedRating(rs.getFloat("AggregatedRating"))
                        .reviewCount(rs.getInt("ReviewCount"))
                        .calories(rs.getFloat("Calories"))
                        .fatContent(rs.getFloat("FatContent"))
                        .saturatedFatContent(rs.getFloat("SaturatedFatContent"))
                        .cholesterolContent(rs.getFloat("CholesterolContent"))
                        .sodiumContent(rs.getFloat("SodiumContent"))
                        .carbohydrateContent(rs.getFloat("CarbohydrateContent"))
                        .fiberContent(rs.getFloat("FiberContent"))
                        .sugarContent(rs.getFloat("SugarContent"))
                        .proteinContent(rs.getFloat("ProteinContent"))
                        .recipeServings(parseIntSafe(rs.getString("RecipeServings")))
                        .recipeYield(rs.getString("RecipeYield"))
                        .build(),
                recipeId
        );
    }

    private void validateActiveUser(AuthInfo auth) {
        if (auth == null || auth.getAuthorId() <= 0) throw new IllegalArgumentException("invalid auth");
        try {
            Map<String, Object> user = jdbcTemplate.queryForMap(
                    "SELECT Password, IsDeleted FROM users WHERE AuthorId = ?", auth.getAuthorId());

            Boolean isDeleted = (Boolean) user.get("IsDeleted");
            if (isDeleted != null && isDeleted) throw new SecurityException("inactive or non-existent user");

            String pwd = (String) user.get("Password");
            if (pwd == null || !pwd.equals(auth.getPassword())) throw new SecurityException("invalid auth");

        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            throw new SecurityException("inactive or non-existent user");
        }
    }

    private void ensureRecipeExists(long recipeId) {
        Integer cnt = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM recipes WHERE RecipeId = ?", Integer.class, recipeId);
        if (cnt == null || cnt == 0) throw new IllegalArgumentException("recipe not exists");
    }

    private void ensureReviewBelongsToRecipe(long reviewId, long recipeId) {
        Integer cnt = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reviews WHERE ReviewId = ? AND RecipeId = ?",
                Integer.class, reviewId, recipeId);
        if (cnt == null || cnt == 0) throw new IllegalArgumentException("review not belongs to recipe");
    }

    private int parseIntSafe(String s) {
        try {
            return s == null ? 0 : Integer.parseInt(s);
        } catch (Exception e) {
            return 0;
        }
    }

    private void resetPkSequenceToNext(String tableName) {
        String pkColumn = resolvePrimaryKeyColumn(tableName);

        String tableRef = qualifyForPg(tableName);
        String colRef = quoteIdentIfNeeded(pkColumn);

        Long maxId = jdbcTemplate.queryForObject(
                "SELECT COALESCE(MAX(" + colRef + "), 0) FROM " + tableRef, Long.class);
        long nextValue = (maxId == null ? 1L : maxId + 1L);

        String seqName = jdbcTemplate.queryForObject(
                "SELECT pg_get_serial_sequence(?, ?)::text", String.class, tableRef, colRef);

        if (seqName == null) {
            return;
        }

        jdbcTemplate.queryForObject(
                "SELECT setval(?, ?, FALSE)", Long.class, seqName, nextValue
        );
    }

    private String resolvePrimaryKeyColumn(String tableName) {
        try (Connection c = dataSource.getConnection()) {
            DatabaseMetaData meta = c.getMetaData();
            try (ResultSet rs = meta.getPrimaryKeys(c.getCatalog(), null, tableName)) {
                if (rs.next()) {
                    return rs.getString("COLUMN_NAME");
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to resolve primary key column for table: " + tableName, e);
        }
        throw new IllegalStateException("No primary key found for table: " + tableName);
    }

    private boolean needsQuoting(String ident) {
        if (ident == null || ident.isEmpty()) return true;
        return !ident.matches("[a-z_][a-z0-9_]*");
    }

    private String quoteIdent(String ident) {
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }

    private String quoteIdentIfNeeded(String ident) {
        return needsQuoting(ident) ? quoteIdent(ident) : ident;
    }

    private String qualifyForPg(String name) {
        String[] parts = name.split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) sb.append('.');
            sb.append(quoteIdentIfNeeded(parts[i]));
        }
        return sb.toString();
    }
}
