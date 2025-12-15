// Java
package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.ReviewService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class ReviewServiceImpl implements ReviewService {

    private final JdbcTemplate jdbcTemplate;

    @Override
    @Transactional
    public long addReview(AuthInfo auth, long recipeId, int rating, String review) {
        validateActiveUser(auth);
        ensureRecipeExists(recipeId);

        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("rating must be in [1,5]");
        }

        Timestamp now = new Timestamp(System.currentTimeMillis());
        String sql = "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                "VALUES (DEFAULT, ?, ?, ?, ?, ?, ?) RETURNING ReviewId";
        Long reviewId = jdbcTemplate.queryForObject(sql, Long.class, recipeId, auth.getAuthorId(), rating, review, now, now);
        refreshRecipeAggregatedRating(recipeId);
        return reviewId;
    }

    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        validateActiveUser(auth);
        ensureRecipeExists(recipeId);
        ensureReviewBelongsToRecipe(reviewId, recipeId);
        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("rating must be in [1,5]");
        }
        // 仅作者可编辑
        Long authorId = jdbcTemplate.queryForObject("SELECT AuthorId FROM reviews WHERE ReviewId = ?", Long.class, reviewId);
        if (authorId == null || !authorId.equals(auth.getAuthorId())) {
            throw new SecurityException("not review author");
        }

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
        if (authorId == null || !authorId.equals(auth.getAuthorId())) {
            throw new SecurityException("not review author");
        }

        jdbcTemplate.update("DELETE FROM review_likes WHERE ReviewId = ?", reviewId);
        jdbcTemplate.update("DELETE FROM reviews WHERE ReviewId = ?", reviewId);

        refreshRecipeAggregatedRating(recipeId);
    }

    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        validateActiveUser(auth);
        // 检查评论存在
        Long reviewAuthor = jdbcTemplate.queryForObject("SELECT AuthorId FROM reviews WHERE ReviewId = ?", Long.class, reviewId);
        if (reviewAuthor == null) {
            throw new IllegalArgumentException("review not exists");
        }
        if (reviewAuthor.equals(auth.getAuthorId())) {
            throw new SecurityException("cannot like own review");
        }
        // 幂等插入
        jdbcTemplate.update("INSERT INTO review_likes (ReviewId, AuthorId) VALUES (?, ?) ON CONFLICT DO NOTHING",
                reviewId, auth.getAuthorId());

        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM review_likes WHERE ReviewId = ?", Long.class, reviewId);
    }

    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        validateActiveUser(auth);
        // 检查评论存在
        Integer exists = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reviews WHERE ReviewId = ?", Integer.class, reviewId);
        if (exists == null || exists == 0) {
            throw new IllegalArgumentException("review not exists");
        }
        jdbcTemplate.update("DELETE FROM review_likes WHERE ReviewId = ? AND AuthorId = ?", reviewId, auth.getAuthorId());
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM review_likes WHERE ReviewId = ?", Long.class, reviewId);
    }

    @Override
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        if (page < 1 || size <= 0) {
            throw new IllegalArgumentException("invalid page/size");
        }
        ensureRecipeExists(recipeId);

        String orderBy = "DateModified DESC";
        if ("likes_desc".equalsIgnoreCase(sort)) {
            orderBy = "likes_count DESC, DateModified DESC, ReviewId ASC";
        }

        String base = "SELECT r.ReviewId, r.RecipeId, r.AuthorId, r.Rating, r.Review, r.DateSubmitted, r.DateModified, " +
                "       COALESCE(l.cnt, 0) AS likes_count " +
                "FROM reviews r " +
                "LEFT JOIN (SELECT ReviewId, COUNT(*) AS cnt FROM review_likes GROUP BY ReviewId) l ON r.ReviewId = l.ReviewId " +
                "WHERE r.RecipeId = ? ";
        String pageSql = base + "ORDER BY " + orderBy + " LIMIT ? OFFSET ?";
        List<ReviewRecord> items = jdbcTemplate.query(pageSql,
                (rs, i) -> ReviewRecord.builder()
                        .reviewId(rs.getLong("ReviewId"))
                        .recipeId(rs.getLong("RecipeId"))
                        .authorId(rs.getLong("AuthorId"))
                        .rating(rs.getInt("Rating"))
                        .review(rs.getString("Review"))
                        .dateSubmitted(rs.getTimestamp("DateSubmitted"))
                        .dateModified(rs.getTimestamp("DateModified"))
                        .likes(new long[0]) // 可按需扩展返回点赞用户
                        .build(),
                recipeId, size, (page - 1) * size);

        Long total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM reviews WHERE RecipeId = ?", Long.class, recipeId);

        return PageResult.<ReviewRecord>builder()
                .items(items)
                .page(page)
                .size(size)
                .total(total == null ? 0 : total)
                .build();
    }

    @Override
    @Transactional
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        ensureRecipeExists(recipeId);
        // 计算平均值与计数
        Double avg = jdbcTemplate.queryForObject(
                "SELECT ROUND(AVG(Rating)::numeric, 2) FROM reviews WHERE RecipeId = ?", Double.class, recipeId);
        Integer cnt = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reviews WHERE RecipeId = ?", Integer.class, recipeId);

        // 更新 recipes
        jdbcTemplate.update("UPDATE recipes SET AggregatedRating = ?, ReviewCount = ? WHERE RecipeId = ?",
                avg, cnt, recipeId);

        // 返回最新 RecipeRecord（只填充与本方法相关的关键字段，其它可按需补齐）
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

    // -------------- 私有辅助方法，占位，需结合你的用户/认证设计完善 --------------

    private void validateActiveUser(AuthInfo auth) {
        if (auth == null || auth.getAuthorId() <= 0) {
            throw new IllegalArgumentException("invalid auth");
        }
        // 示例校验：用户存在且未删除
        Integer ok = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM users WHERE AuthorId = ? AND COALESCE(IsDeleted, FALSE) = FALSE",
                Integer.class, auth.getAuthorId());
        if (ok == null || ok == 0) {
            throw new SecurityException("inactive or non-existent user");
        }
    }

    private void ensureRecipeExists(long recipeId) {
        Integer cnt = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM recipes WHERE RecipeId = ?", Integer.class, recipeId);
        if (cnt == null || cnt == 0) {
            throw new IllegalArgumentException("recipe not exists");
        }
    }

    private void ensureReviewBelongsToRecipe(long reviewId, long recipeId) {
        Integer cnt = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reviews WHERE ReviewId = ? AND RecipeId = ?",
                Integer.class, reviewId, recipeId);
        if (cnt == null || cnt == 0) {
            throw new IllegalArgumentException("review not belongs to recipe");
        }
    }

    private int parseIntSafe(String s) {
        try {
            return s == null ? 0 : Integer.parseInt(s);
        } catch (Exception e) {
            return 0;
        }
    }
}
