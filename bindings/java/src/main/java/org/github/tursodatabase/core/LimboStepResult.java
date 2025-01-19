package org.github.tursodatabase.core;

import java.util.Arrays;

import org.github.tursodatabase.annotations.NativeInvocation;

/**
 * Represents the step result of limbo's statement's step function.
 */
public class LimboStepResult {
    public static final int STEP_RESULT_ID_ROW = 10;
    public static final int STEP_RESULT_ID_IO = 20;
    public static final int STEP_RESULT_ID_DONE = 30;
    public static final int STEP_RESULT_ID_INTERRUPT = 40;
    public static final int STEP_RESULT_ID_BUSY = 50;
    public static final int STEP_RESULT_ID_ERROR = 60;

    // Identifier for limbo's StepResult
    private final int stepResultId;
    private final Object[] result;

    @NativeInvocation
    public LimboStepResult(int stepResultId, Object[] result) {
        this.stepResultId = stepResultId;
        this.result = result;
    }

    public boolean isDone() {
        return stepResultId == STEP_RESULT_ID_DONE;
    }

    @Override
    public String toString() {
        return "LimboStepResult{" +
               "stepResultId=" + stepResultId +
               ", result=" + Arrays.toString(result) +
               '}';
    }
}
