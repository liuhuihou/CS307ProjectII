package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

@Service
@Slf4j
public class UserServiceImpl implements UserService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public long register(RegisterUserReq req) {
        if (req == null || req.getName() == null || req.getName().isEmpty() ||
            req.getGender() == null || req.getGender() == RegisterUserReq.Gender.UNKNOWN ||
            req.getPassword() == null || req.getPassword().isEmpty() || req.getBirthday() == null) {
            return -1;
        }

        // Check name uniqueness
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM users WHERE AuthorName = ?", Integer.class, req.getName());
        if (count != null && count > 0) {
            return -1;
        }

        // Calculate age
        int age;
        try {
            LocalDate birthDate = parseDate(req.getBirthday());
            if (birthDate == null) return -1;

            age = Period.between(birthDate, LocalDate.now()).getYears();
            if (age <= 0) return -1;
        } catch (Exception e) {
            return -1;
        }

        // Generate ID
        Long maxId = jdbcTemplate.queryForObject("SELECT MAX(AuthorId) FROM users", Long.class);
        long newId = (maxId == null ? 0 : maxId) + 1;

        String genderStr = req.getGender() == RegisterUserReq.Gender.MALE ? "Male" : "Female";

        String sql = "INSERT INTO users (AuthorId, AuthorName, Gender, Age, Password, IsDeleted) VALUES (?, ?, ?, ?, ?, ?)";
        jdbcTemplate.update(sql, newId, req.getName(), genderStr, age, req.getPassword(), false);

        return newId;
    }

    private LocalDate parseDate(String dateStr) {
        if (dateStr == null) return null;
        try {
            return LocalDate.parse(dateStr);
        } catch (DateTimeParseException e) {
            try {
                return LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy/MM/dd"));
            } catch (DateTimeParseException e2) {
                return null;
            }
        }
    }

    @Override
    public long login(AuthInfo auth) {
        if (auth == null || auth.getPassword() == null || auth.getPassword().isEmpty() || auth.getAuthorId() <= 0) {
            return -1;
        }
        try {
            Map<String, Object> user = jdbcTemplate.queryForMap("SELECT Password, IsDeleted FROM users WHERE AuthorId = ?", auth.getAuthorId());
            Boolean isDeleted = (Boolean) user.get("IsDeleted");
            if (isDeleted != null && isDeleted) return -1;

            String storedPwd = (String) user.get("Password");
            if (storedPwd != null && storedPwd.equals(auth.getPassword())) {
                return auth.getAuthorId();
            }
        } catch (EmptyResultDataAccessException e) {
            return -1;
        }
        return -1;
    }

    @Override
    @Transactional
    public boolean deleteAccount(AuthInfo auth, long userId) {
        if (!validateAuth(auth)) {
            throw new SecurityException("Invalid auth");
        }
        if (auth.getAuthorId() != userId) {
             throw new SecurityException("Cannot delete other's account");
        }

        try {
            Boolean isDeleted = jdbcTemplate.queryForObject("SELECT IsDeleted FROM users WHERE AuthorId = ?", Boolean.class, userId);
            if (isDeleted == null || isDeleted) return false;
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("User not found");
        }

        jdbcTemplate.update("UPDATE users SET IsDeleted = true WHERE AuthorId = ?", userId);
        jdbcTemplate.update("DELETE FROM user_follows WHERE FollowerId = ? OR FollowingId = ?", userId, userId);
        return true;
    }

    @Override
    @Transactional
    public boolean follow(AuthInfo auth, long followeeId) {
        if (!validateAuth(auth)) {
            throw new SecurityException("Invalid auth");
        }
        if (auth.getAuthorId() == followeeId) {
            throw new SecurityException("Cannot follow self");
        }

        try {
            Boolean isDeleted = jdbcTemplate.queryForObject("SELECT IsDeleted FROM users WHERE AuthorId = ?", Boolean.class, followeeId);
            if (isDeleted == null || isDeleted) throw new SecurityException("Followee not found or deleted");
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("Followee not found");
        }

        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM user_follows WHERE FollowerId = ? AND FollowingId = ?", Integer.class, auth.getAuthorId(), followeeId);
        if (count != null && count > 0) {
            jdbcTemplate.update("DELETE FROM user_follows WHERE FollowerId = ? AND FollowingId = ?", auth.getAuthorId(), followeeId);
            return false;
        } else {
            jdbcTemplate.update("INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?)", auth.getAuthorId(), followeeId);
            return true;
        }
    }

    @Override
    public UserRecord getById(long userId) {
        try {
            UserRecord record = jdbcTemplate.queryForObject("SELECT * FROM users WHERE AuthorId = ? AND IsDeleted = false",
                (rs, rowNum) -> {
                    UserRecord u = new UserRecord();
                    u.setAuthorId(rs.getLong("AuthorId"));
                    u.setAuthorName(rs.getString("AuthorName"));
                    u.setGender(rs.getString("Gender"));
                    u.setAge(rs.getInt("Age"));
                    u.setPassword(rs.getString("Password"));
                    u.setDeleted(rs.getBoolean("IsDeleted"));
                    return u;
                }, userId);

            if (record != null) {
                Integer followers = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM user_follows WHERE FollowingId = ?", Integer.class, userId);
                Integer following = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM user_follows WHERE FollowerId = ?", Integer.class, userId);
                record.setFollowers(followers != null ? followers : 0);
                record.setFollowing(following != null ? following : 0);

                List<Long> followerList = jdbcTemplate.queryForList("SELECT FollowerId FROM user_follows WHERE FollowingId = ?", Long.class, userId);
                record.setFollowerUsers(followerList.stream().mapToLong(Long::longValue).toArray());

                List<Long> followingList = jdbcTemplate.queryForList("SELECT FollowingId FROM user_follows WHERE FollowerId = ?", Long.class, userId);
                record.setFollowingUsers(followingList.stream().mapToLong(Long::longValue).toArray());
            }
            return record;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public void updateProfile(AuthInfo auth, String gender, Integer age) {
        if (!validateAuth(auth)) {
            throw new SecurityException("Invalid auth");
        }

        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder("UPDATE users SET ");
        boolean update = false;

        if (gender != null && (gender.equals("Male") || gender.equals("Female"))) {
            sql.append("Gender = ?, ");
            params.add(gender);
            update = true;
        }

        if (age != null && age > 0) {
            sql.append("Age = ?, ");
            params.add(age);
            update = true;
        }

        if (update) {
            sql.setLength(sql.length() - 2);
            sql.append(" WHERE AuthorId = ?");
            params.add(auth.getAuthorId());
            jdbcTemplate.update(sql.toString(), params.toArray());
        }
    }

    @Override
    public PageResult<FeedItem> feed(AuthInfo auth, int page, int size, String category) {
        if (!validateAuth(auth)) {
            throw new SecurityException("Invalid auth");
        }
        if (size > 200) size = 200;
        if (size < 1) size = 1;
        if (page < 1) page = 1;

        StringBuilder sql = new StringBuilder(
                "SELECT r.RecipeId, r.Name, r.AuthorId, u.AuthorName, r.DatePublished, r.AggregatedRating, r.ReviewCount " +
                "FROM recipes r " +
                "JOIN users u ON r.AuthorId = u.AuthorId " +
                "JOIN user_follows uf ON r.AuthorId = uf.FollowingId " +
                "WHERE uf.FollowerId = ?");

        List<Object> params = new ArrayList<>();
        params.add(auth.getAuthorId());

        if (category != null && !category.isEmpty()) {
            sql.append(" AND r.RecipeCategory = ?");
            params.add(category);
        }

        Long total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM (" + sql.toString() + ") as t", Long.class, params.toArray());
        if (total == null) total = 0L;

        sql.append(" ORDER BY r.DatePublished DESC, r.RecipeId DESC LIMIT ? OFFSET ?");
        params.add(size);
        params.add((page - 1) * size);

        List<FeedItem> items = jdbcTemplate.query(sql.toString(), (rs, rowNum) -> {
            return FeedItem.builder()
                    .recipeId(rs.getLong("RecipeId"))
                    .name(rs.getString("Name"))
                    .authorId(rs.getLong("AuthorId"))
                    .authorName(rs.getString("AuthorName"))
                    .datePublished(rs.getTimestamp("DatePublished", Calendar.getInstance(TimeZone.getTimeZone("UTC"))).toInstant())
                    .aggregatedRating(rs.getDouble("AggregatedRating"))
                    .reviewCount(rs.getInt("ReviewCount"))
                    .build();
        }, params.toArray());

        return new PageResult<>(items, page, size, total);
    }

    @Override
    public Map<String, Object> getUserWithHighestFollowRatio() {
        String sql = "WITH FollowerCounts AS (" +
                "    SELECT FollowingId as AuthorId, COUNT(*) as cnt FROM user_follows GROUP BY FollowingId" +
                "), FollowingCounts AS (" +
                "    SELECT FollowerId as AuthorId, COUNT(*) as cnt FROM user_follows GROUP BY FollowerId" +
                ") " +
                "SELECT u.AuthorId, u.AuthorName, " +
                "CAST(COALESCE(fc.cnt, 0) AS FLOAT) / fgc.cnt as ratio " +
                "FROM users u " +
                "JOIN FollowingCounts fgc ON u.AuthorId = fgc.AuthorId " +
                "LEFT JOIN FollowerCounts fc ON u.AuthorId = fc.AuthorId " +
                "WHERE u.IsDeleted = false AND fgc.cnt > 0 " +
                "ORDER BY ratio DESC, u.AuthorId ASC " +
                "LIMIT 1";

        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                Map<String, Object> map = new HashMap<>();
                map.put("AuthorId", rs.getLong("AuthorId"));
                map.put("AuthorName", rs.getString("AuthorName"));
                map.put("Ratio", rs.getDouble("ratio"));
                return map;
            });
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    private boolean validateAuth(AuthInfo auth) {
        return login(auth) != -1;
    }
}