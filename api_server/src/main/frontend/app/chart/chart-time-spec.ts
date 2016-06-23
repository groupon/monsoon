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
