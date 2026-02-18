package com.databricks.zerobus;

/**
 * Represents the lifecycle state of a ZerobusStream.
 *
 * <p>State transitions typically follow this pattern:
 *
 * <pre>
 * UNINITIALIZED → OPENED → FLUSHING → CLOSED
 *                    ↓
 *                RECOVERING (on failure, if recovery enabled)
 *                    ↓
 *                OPENED or FAILED
 * </pre>
 */
public enum StreamState {
  /** Stream created but not yet initialized. */
  UNINITIALIZED,

  /** Stream is open and accepting records. */
  OPENED,

  /** Stream is flushing pending records before closing. */
  FLUSHING,

  /** Stream is recovering from a failure (automatic retry in progress). */
  RECOVERING,

  /** Stream has been gracefully closed. */
  CLOSED,

  /** Stream has failed and cannot be recovered. */
  FAILED
}
