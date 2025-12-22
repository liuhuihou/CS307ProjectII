package io.sustc.ui.security;

import org.springframework.http.HttpStatus;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public final class AuthFilter extends OncePerRequestFilter {

    private final JwtUtil jwt;

    public AuthFilter(JwtUtil jwt) {
        this.jwt = jwt;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        // 修复：移除 path.startsWith("/") 条件，避免所有请求都被排除在过滤之外
        return path.startsWith("/ui/auth/") || path.startsWith("/swagger") || path.startsWith("/v3/api-docs");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp, FilterChain chain) {
        try {
            String auth = req.getHeader("Authorization");
            String token = null;
            if (auth != null && auth.startsWith("Bearer ")) {
                token = auth.substring("Bearer ".length());
            }

            Long userId = jwt.verifyAndGetUserId(token);
            if (userId == null) {
                resp.setStatus(HttpStatus.UNAUTHORIZED.value());
                resp.setContentType("application/json;charset=utf-8");
                resp.getWriter().write("{\"ok\":false,\"message\":\"unauthorized\"}");
                return;
            }
            req.setAttribute("uiUserId", userId);
            chain.doFilter(req, resp);
        } catch (Exception e) {
            // 改进：在生产环境中应该使用日志框架记录错误
            // logger.error("AuthFilter error", e);

            try {
                resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
                resp.setContentType("application/json;charset=utf-8");
                resp.getWriter().write("{\"ok\":false,\"message\":\"auth_filter_error\"}");
            } catch (Exception ignored) {
                // 忽略写入响应时的异常
            }
        }
    }
}