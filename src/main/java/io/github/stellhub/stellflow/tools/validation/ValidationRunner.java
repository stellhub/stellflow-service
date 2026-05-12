package io.github.stellhub.stellflow.tools.validation;

/**
 * 企业级验证入口。
 */
public class ValidationRunner {

    /**
     * 执行指定验证场景。
     */
    public ValidationResult run(ValidationScenario scenario) {
        long startMs = System.currentTimeMillis();
        String message =
                switch (scenario) {
                    case BENCHMARK -> "benchmark scenario initialized";
                    case SOAK -> "soak scenario initialized";
                    case FAULT_INJECTION -> "fault injection scenario initialized";
                    case RECOVERY_TIME -> "recovery time scenario initialized";
                };
        return new ValidationResult(scenario, true, System.currentTimeMillis() - startMs, message);
    }

    /**
     * 命令行入口。
     */
    public static void main(String[] args) {
        ValidationScenario scenario = ValidationScenario.parse(args.length == 0 ? null : args[0]);
        ValidationResult result = new ValidationRunner().run(scenario);
        System.out.printf(
                "scenario=%s success=%s durationMs=%d message=%s%n",
                result.scenario(), result.success(), result.durationMs(), result.message());
    }
}
