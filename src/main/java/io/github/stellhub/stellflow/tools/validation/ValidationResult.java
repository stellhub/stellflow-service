package io.github.stellhub.stellflow.tools.validation;

/**
 * 验证结果。
 */
public record ValidationResult(
        ValidationScenario scenario, boolean success, long durationMs, String message) {}
