package io.github.stellhub.stellflow.tools.validation;

/**
 * 验证场景。
 */
public enum ValidationScenario {
    BENCHMARK,
    SOAK,
    FAULT_INJECTION,
    RECOVERY_TIME;

    /**
     * 解析场景名称。
     */
    public static ValidationScenario parse(String value) {
        if (value == null || value.isBlank()) {
            return BENCHMARK;
        }
        return ValidationScenario.valueOf(value.trim().toUpperCase().replace('-', '_'));
    }
}
