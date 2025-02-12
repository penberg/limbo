package tech.turso.core;

import java.util.Arrays;
import tech.turso.annotations.NativeInvocation;
import tech.turso.annotations.Nullable;

/** Represents the step result of limbo's statement's step function. */
public class LimboStepResult {
  private static final int STEP_RESULT_ID_ROW = 10;
  private static final int STEP_RESULT_ID_IO = 20;
  private static final int STEP_RESULT_ID_DONE = 30;
  private static final int STEP_RESULT_ID_INTERRUPT = 40;
  // Indicates that the database file could not be written because of concurrent activity by some
  // other connection
  private static final int STEP_RESULT_ID_BUSY = 50;
  private static final int STEP_RESULT_ID_ERROR = 60;

  // Identifier for limbo's StepResult
  private final int stepResultId;
  @Nullable private final Object[] result;

  @NativeInvocation(invokedFrom = "limbo_statement.rs")
  public LimboStepResult(int stepResultId) {
    this.stepResultId = stepResultId;
    this.result = null;
  }

  @NativeInvocation(invokedFrom = "limbo_statement.rs")
  public LimboStepResult(int stepResultId, Object[] result) {
    this.stepResultId = stepResultId;
    this.result = result;
  }

  public boolean isRow() {
    return stepResultId == STEP_RESULT_ID_ROW;
  }

  public boolean isDone() {
    return stepResultId == STEP_RESULT_ID_DONE;
  }

  public boolean isInInvalidState() {
    // current implementation doesn't allow STEP_RESULT_ID_IO to be returned
    return stepResultId == STEP_RESULT_ID_IO
        || stepResultId == STEP_RESULT_ID_INTERRUPT
        || stepResultId == STEP_RESULT_ID_BUSY
        || stepResultId == STEP_RESULT_ID_ERROR;
  }

  @Nullable
  public Object[] getResult() {
    return result;
  }

  @Override
  public String toString() {
    return "LimboStepResult{"
        + "stepResultName="
        + getStepResultName()
        + ", result="
        + Arrays.toString(result)
        + '}';
  }

  private String getStepResultName() {
    switch (stepResultId) {
      case STEP_RESULT_ID_ROW:
        return "ROW";
      case STEP_RESULT_ID_IO:
        return "IO";
      case STEP_RESULT_ID_DONE:
        return "DONE";
      case STEP_RESULT_ID_INTERRUPT:
        return "INTERRUPT";
      case STEP_RESULT_ID_BUSY:
        return "BUSY";
      case STEP_RESULT_ID_ERROR:
        return "ERROR";
      default:
        return "UNKNOWN";
    }
  }
}
