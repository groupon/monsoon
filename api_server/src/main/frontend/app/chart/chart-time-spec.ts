/*
 * Time specifications:
 * - begin time only (stream from begin until end-of-data).
 * - begin time and end time (stream from begin until end).
 * - begin time and duration (treat as end = begin + duration).
 * - end time and duration (treat as begin = end - duration).
 * - duration only (treat as begin = now() - duration, no end, no duration).
 *
 * Duration-only is to be interpreted as a sliding window, always displaying
 * data between the end-of-data - duration .. end-of-data.
 */
export class ChartTimeSpec {
  constructor(public duration_seconds: Number, public end?: Date, public stepsize_seconds?: Number) {}

  public getEnd() : Date {
    return (this.end ? this.end : new Date());
  }

  public getBegin() : Date {
    return new Date(this.getEnd().getTime() - 1000 * Number(this.duration_seconds));
  }

  public getStepsizeMsec() : Number {
    return (this.stepsize_seconds ? 1000 * Number(this.stepsize_seconds) : null);
  }
}
