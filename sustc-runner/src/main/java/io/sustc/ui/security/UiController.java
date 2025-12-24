package io.sustc.ui.security;

import io.sustc.dto.*;
import io.sustc.service.RecipeService;
import io.sustc.service.ReviewService;
import io.sustc.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/ui")
@RequiredArgsConstructor
public class UiController {

    private final JdbcTemplate jdbcTemplate;
    private final JwtUtil jwtUtil;
    private final UserService userService;
    private final RecipeService recipeService;
    private final ReviewService reviewService;

    /**
     * 用户注册
     */
    @PostMapping("/auth/register")
    public Map<String, Object> register(@RequestBody Map<String, Object> body) {
        try {
            RegisterUserReq req = RegisterUserReq.builder()
                    .name(String.valueOf(body.getOrDefault("name", "")))
                    .password(String.valueOf(body.getOrDefault("password", "")))
                    .gender(parseGender(String.valueOf(body.getOrDefault("gender", ""))))
                    .birthday(String.valueOf(body.getOrDefault("birthday", "")))
                    .build();

            long userId = userService.register(req);
            if (userId == -1) {
                return Map.of("ok", false, "error", "注册失败，请检查输入信息");
            }

            String token = jwtUtil.issueToken(userId, 24 * 3600);
            return Map.of("ok", true, "token", token, "userId", userId, "message", "注册成功");
        } catch (Exception e) {
            return Map.of("ok", false, "error", "注册失败: " + e.getMessage());
        }
    }

    /**
     * 用户登录
     */
    @PostMapping("/auth/login")
    public Map<String, Object> login(@RequestBody Map<String, Object> body) {
        try {
            String username = String.valueOf(body.getOrDefault("username", ""));
            String password = String.valueOf(body.getOrDefault("password", ""));

            // 根据用户名获取用户ID
            Long userId = getUserIdByUsername(username);
            if (userId == null) {
                return Map.of("ok", false, "error", "用户名不存在");
            }

            AuthInfo auth = new AuthInfo();
            auth.setAuthorId(userId);
            auth.setPassword(password);

            long loginResult = userService.login(auth);
            if (loginResult == -1) {
                return Map.of("ok", false, "error", "登录失败，用户名或密码错误");
            }

            String token = jwtUtil.issueToken(userId, 24 * 3600);
            return Map.of("ok", true, "token", token, "userId", userId);
        } catch (Exception e) {
            return Map.of("ok", false, "error", "登录失败: " + e.getMessage());
        }
    }

    /**
     * 获取用户信息
     */
    @GetMapping("/users/{userId}")
    public Map<String, Object> getUserInfo(@PathVariable long userId) {
        try {
            var userRecord = userService.getById(userId);
            if (userRecord == null) {
                return Map.of("ok", false, "error", "用户不存在");
            }
            return Map.of("ok", true, "user", Map.of(
                    "id", userRecord.getAuthorId(),
                    "name", userRecord.getAuthorName(),
                    "gender", userRecord.getGender(),
                    "age", userRecord.getAge(),
                    "followers", userRecord.getFollowers(),
                    "following", userRecord.getFollowing()
            ));
        } catch (Exception e) {
            return Map.of("ok", false, "error", "获取用户信息失败: " + e.getMessage());
        }
    }

    /**
     * 更新用户资料
     */
    @PutMapping("/users/profile")
    public Map<String, Object> updateProfile(@RequestBody Map<String, Object> body, HttpServletRequest req) {
        try {
            long userId = (long) req.getAttribute("uiUserId");
            AuthInfo auth = createAuthInfo(userId);

            String gender = body.containsKey("gender") ? String.valueOf(body.get("gender")) : null;
            Integer age = body.containsKey("age") ? Integer.valueOf(String.valueOf(body.get("age"))) : null;

            userService.updateProfile(auth, gender, age);
            return Map.of("ok", true, "message", "资料更新成功");
        } catch (Exception e) {
            return Map.of("ok", false, "error", "更新资料失败: " + e.getMessage());
        }
    }

    /**
     * 关注/取消关注用户
     */
    @PostMapping("/users/{followeeId}/follow")
    public Map<String, Object> followUser(@PathVariable long followeeId, HttpServletRequest req) {
        try {
            long userId = (long) req.getAttribute("uiUserId");
            AuthInfo auth = createAuthInfo(userId);

            boolean result = userService.follow(auth, followeeId);
            String action = result ? "关注" : "取消关注";
            return Map.of("ok", true, "action", action, "message", action + "成功");
        } catch (Exception e) {
            return Map.of("ok", false, "error", "操作失败: " + e.getMessage());
        }
    }

    /**
     * 删除用户账户
     */
    @DeleteMapping("/users/{userId}")
    public Map<String, Object> deleteAccount(@PathVariable long userId, HttpServletRequest req) {
        try {
            long currentUserId = (long) req.getAttribute("uiUserId");
            AuthInfo auth = createAuthInfo(currentUserId);

            if (currentUserId != userId) {
                return Map.of("ok", false, "error", "只能删除自己的账户");
            }

            boolean result = userService.deleteAccount(auth, userId);
            return Map.of("ok", result, "message", result ? "账户删除成功" : "删除失败");
        } catch (Exception e) {
            return Map.of("ok", false, "error", "删除账户失败: " + e.getMessage());
        }
    }

    /**
     * 获取食谱列表
     */
    @GetMapping("/recipes")
    public Map<String, Object> listRecipes(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int pageSize,
            @RequestParam(required = false) String q,
            @RequestParam(required = false) String category
    ) {
        try {
            var result = recipeService.searchRecipes(q, category, null, page, pageSize, "date_published DESC");
            if (result == null || result.getItems() == null) {
                return Map.of("ok", true, "page", page, "pageSize", pageSize, "items", List.of(), "total", 0);
            }

            List<Map<String, Object>> items = result.getItems().stream()
                    .map(recipe -> {
                        // 使用HashMap替代Map.of()以避免类型转换问题
                        Map<String, Object> item = new HashMap<>();
                        item.put("id", recipe.getRecipeId());
                        item.put("title", recipe.getName());
                        item.put("author", recipe.getAuthorName());
                        item.put("category", recipe.getRecipeCategory());
                        item.put("rating", recipe.getAggregatedRating());
                        item.put("createdAt", recipe.getDatePublished());
                        return item;
                    })
                    .toList();
            return Map.of("ok", true, "page", page, "pageSize", pageSize, "items", items, "total", result.getTotal());
        } catch (Exception e) {
            return Map.of("ok", false, "error", "获取食谱列表失败: " + e.getMessage());
        }
    }

    /**
     * 获取食谱详情
     */
    @GetMapping("/recipes/{id}")
    public Map<String, Object> getRecipe(@PathVariable long id) {
        try {
            var recipe = recipeService.getRecipeById(id);
            if (recipe == null) {
                return Map.of("ok", false, "error", "食谱不存在");
            }
            return Map.of("ok", true, "recipe", Map.of(
                    "id", recipe.getRecipeId(),
                    "title", recipe.getName(),
                    "description", recipe.getDescription(),
                    "ingredients", recipe.getRecipeIngredientParts(),
                    "category", recipe.getRecipeCategory(),
                    "calories", recipe.getCalories(),
                    "cookTime", recipe.getCookTime(),
                    "author", recipe.getAuthorName(),
                    "authorId", recipe.getAuthorId(),
                    "createdAt", recipe.getDatePublished()
            ));
        } catch (Exception e) {
            return Map.of("ok", false, "error", "获取食谱详情失败: " + e.getMessage());
        }
    }

    /**
     * 创建食谱
     */
    @PostMapping("/recipes")
    public Map<String, Object> createRecipe(@RequestBody Map<String, Object> body, HttpServletRequest req) {
        try {
            long userId = (long) req.getAttribute("uiUserId");
            AuthInfo auth = createAuthInfo(userId);

            RecipeRecord recipeRecord = RecipeRecord.builder()
                    .name(String.valueOf(body.get("title")))
                    .description(String.valueOf(body.get("description")))
                    .recipeIngredientParts(new String[]{String.valueOf(body.get("ingredients"))})
                    .recipeCategory(String.valueOf(body.get("category")))
                    .calories(body.containsKey("calories") ? Float.valueOf(String.valueOf(body.get("calories"))) : null)
                    .cookTime(body.containsKey("cookTime") ? String.valueOf(body.get("cookTime")) : null)
                    .build();

            long recipeId = recipeService.createRecipe(recipeRecord, auth);
            return Map.of("ok", true, "recipeId", recipeId, "message", "食谱创建成功");
        } catch (Exception e) {
            return Map.of("ok", false, "error", "创建食谱失败: " + e.getMessage());
        }
    }

    /**
     * 获取食谱评论
     */
    @GetMapping("/recipes/{id}/comments")
    public Map<String, Object> listComments(@PathVariable long id, HttpServletRequest req) {
        try {
            Long currentUserId = (Long) req.getAttribute("uiUserId");

            var reviews = reviewService.listByRecipe(id, 1, 20, "date_desc");
            if (reviews == null || reviews.getItems() == null) {
                return Map.of("ok", true, "comments", List.of());
            }

            List<Map<String, Object>> items = reviews.getItems().stream()
                    .map(review -> {
                        Object rating = review.getRating();
                        Double ratingValue = null;
                        if (rating != null) {
                            if (rating instanceof Number) {
                                ratingValue = ((Number) rating).doubleValue();
                            } else {
                                try {
                                    ratingValue = Double.parseDouble(String.valueOf(rating));
                                } catch (NumberFormatException e) {
                                    ratingValue = 0.0;
                                }
                            }
                        }

                        // 使用HashMap替代Map.of()以避免类型转换问题
                        Map<String, Object> item = new HashMap<>();
                        item.put("id", review.getReviewId());
                        item.put("authorId", review.getAuthorId());
                        item.put("authorName", getUsernameById(review.getAuthorId()));
                        item.put("content", review.getReview());
                        item.put("rating", ratingValue);
                        item.put("createdAt", review.getDateSubmitted());
                        item.put("likes", review.getLikes() != null ? review.getLikes().length : 0);

                        boolean liked = false;
                        if (currentUserId != null) {
                            liked = isLikedByUser(review.getReviewId(), currentUserId);
                        }
                        item.put("liked", liked);

                        return item;
                    })
                    .toList();
            return Map.of("ok", true, "comments", items);
        } catch (Exception e) {
            return Map.of("ok", false, "error", "获取评论失败: " + e.getMessage());
        }
    }

    /**
     * 添加评论
     */
    @PostMapping("/recipes/{id}/comments")
    public Map<String, Object> addComment(@PathVariable long id, @RequestBody Map<String, Object> body, HttpServletRequest req) {
        try {
            long userId = (long) req.getAttribute("uiUserId");
            AuthInfo auth = createAuthInfo(userId);

            Object ratingObj = body.getOrDefault("rating", "5");
            int rating;
            if (ratingObj instanceof Number) {
                rating = ((Number) ratingObj).intValue();
            } else {
                try {
                    rating = Integer.parseInt(String.valueOf(ratingObj));
                } catch (NumberFormatException e) {
                    return Map.of("ok", false, "error", "评分格式不正确，请输入1-5之间的整数");
                }
            }

            if (rating < 1 || rating > 5) {
                return Map.of("ok", false, "error", "评分必须在1-5之间");
            }

            String content = String.valueOf(body.getOrDefault("content", ""));
            if (content.trim().isEmpty()) {
                return Map.of("ok", false, "error", "评论内容不能为空");
            }

            long reviewId = reviewService.addReview(auth, id, rating, content);
            return Map.of("ok", true, "reviewId", reviewId, "message", "评论添加成功");
        } catch (Exception e) {
            return Map.of("ok", false, "error", "添加评论失败: " + e.getMessage());
        }
    }

    /**
     * 点赞评论
     */
    @PostMapping("/comments/{reviewId}/like")
    public Map<String, Object> likeComment(@PathVariable long reviewId, HttpServletRequest req) {
        try {
            long userId = (long) req.getAttribute("uiUserId");
            AuthInfo auth = createAuthInfo(userId);

            long result = reviewService.likeReview(auth, reviewId);
            return Map.of("ok", true, "likes", result, "message", "点赞成功");
        } catch (Exception e) {
            return Map.of("ok", false, "error", "点赞失败: " + e.getMessage());
        }
    }

    /**
     * 取消点赞评论
     */
    @PostMapping("/comments/{reviewId}/unlike")
    public Map<String, Object> unlikeComment(@PathVariable long reviewId, HttpServletRequest req) {
        try {
            long userId = (long) req.getAttribute("uiUserId");
            AuthInfo auth = createAuthInfo(userId);

            long result = reviewService.unlikeReview(auth, reviewId);
            return Map.of("ok", true, "likes", result, "message", "取消点赞成功");
        } catch (Exception e) {
            return Map.of("ok", false, "error", "取消点赞失败: " + e.getMessage());
        }
    }

    /**
     * 获取用户动态
     */
    @GetMapping("/feed")
    public Map<String, Object> getFeed(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String category,
            HttpServletRequest req
    ) {
        try {
            long userId = (long) req.getAttribute("uiUserId");
            AuthInfo auth = createAuthInfo(userId);

            var feed = userService.feed(auth, page, size, category);
            if (feed == null || feed.getItems() == null) {
                return Map.of("ok", true, "feed", List.of(), "page", page, "size", size);
            }
            return Map.of("ok", true, "feed", feed.getItems(), "page", page, "size", size);
        } catch (Exception e) {
            return Map.of("ok", false, "error", "获取动态失败: " + e.getMessage());
        }
    }

    /**
     * 获取用户的关注列表
     */
    @GetMapping("/users/{userId}/following")
    public Map<String, Object> getFollowing(@PathVariable long userId) {
        try {
            String sql = "SELECT u.AuthorId as id, u.AuthorName as name FROM users u " +
                         "JOIN user_follows f ON u.AuthorId = f.followingid " +
                         "WHERE f.followerid = ?";
            List<Map<String, Object>> following = jdbcTemplate.queryForList(sql, userId);
            return Map.of("ok", true, "items", following);
        } catch (Exception e) {
            return Map.of("ok", false, "error", "获取关注列表失败: " + e.getMessage());
        }
    }

    /**
     * 获取用户的粉丝列表
     */
    @GetMapping("/users/{userId}/followers")
    public Map<String, Object> getFollowers(@PathVariable long userId) {
        try {
            String sql = "SELECT u.AuthorId as id, u.AuthorName as name FROM users u " +
                         "JOIN user_follows f ON u.AuthorId = f.followerid " +
                         "WHERE f.followingid = ?";
            List<Map<String, Object>> followers = jdbcTemplate.queryForList(sql, userId);
            return Map.of("ok", true, "items", followers);
        } catch (Exception e) {
            return Map.of("ok", false, "error", "获取粉丝列表失败: " + e.getMessage());
        }
    }

    // 辅助方法
    private RegisterUserReq.Gender parseGender(String genderStr) {
        return switch (genderStr.toLowerCase()) {
            case "male", "男" -> RegisterUserReq.Gender.MALE;
            case "female", "女" -> RegisterUserReq.Gender.FEMALE;
            default -> RegisterUserReq.Gender.UNKNOWN;
        };
    }

    private AuthInfo createAuthInfo(long userId) {
        AuthInfo auth = new AuthInfo();
        auth.setAuthorId(userId);
        // 从数据库获取密码
        try {
            String sql = "SELECT Password FROM users WHERE AuthorId = ? AND IsDeleted = false";
            String password = jdbcTemplate.queryForObject(sql, String.class, userId);
            auth.setPassword(password);
        } catch (Exception e) {
            // 如果找不到用户或密码，保持为空，Service层可能会报错
            System.err.println("Failed to fetch password for user " + userId + ": " + e.getMessage());
        }
        return auth;
    }

    private Long getUserIdByUsername(String username) {
        try {
            String sql = "SELECT AuthorId FROM users WHERE AuthorName = ? AND IsDeleted = false";
            return jdbcTemplate.queryForObject(sql, Long.class, username);
        } catch (Exception e) {
            return null;
        }
    }

    private String getUsernameById(long userId) {
        try {
            String sql = "SELECT AuthorName FROM users WHERE AuthorId = ? AND IsDeleted = false";
            return jdbcTemplate.queryForObject(sql, String.class, userId);
        } catch (Exception e) {
            return "未知用户";
        }
    }

    private boolean isLikedByUser(long reviewId, long userId) {
        try {
            String sql = "SELECT COUNT(*) FROM review_likes WHERE reviewid = ? AND authorid = ?";
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, reviewId, userId);
            return count != null && count > 0;
        } catch (Exception e) {
            return false;
        }
    }
}
