/**
 * Error handling for SQLGlot-TS
 */

/**
 * Error level controls how the parser handles errors.
 */
export enum ErrorLevel {
  /** Ignore all errors (collect but continue silently) */
  IGNORE = "IGNORE",
  /** Log all errors (collect, warn, continue) */
  WARN = "WARN",
  /** Collect all errors and raise a single exception at the end */
  RAISE = "RAISE",
  /** Immediately raise an exception on the first error (default) */
  IMMEDIATE = "IMMEDIATE",
}

/**
 * Error thrown by the parser when encountering invalid SQL.
 */
export class ParseError extends Error {
  constructor(
    message: string,
    public readonly errors: ParseErrorDetail[] = [],
  ) {
    super(message)
    this.name = "ParseError"
  }
}

/**
 * Details about a specific parse error.
 */
export interface ParseErrorDetail {
  description: string
  line?: number
  col?: number
  startContext?: string
  highlight?: string
  endContext?: string
}
