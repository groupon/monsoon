import { Injectable,
         OnInit,
         OnDestroy }       from '@angular/core';
import { ApiQueryEncoder } from '../ApiQueryEncoder';
import { URLSearchParams } from '@angular/http';
import { Router,
         ActivatedRoute }  from '@angular/router';
import { Subject }         from 'rxjs/Subject';
import { Subscription }    from 'rxjs/Subscription';
import { Observable }      from 'rxjs/Observable';
import                          'rxjs/add/observable/defer';
import                          'rxjs/add/observable/of';
import                          'rxjs/add/operator/concat';
import                          'rxjs/add/operator/distinctUntilChanged';
import                          'rxjs/add/operator/share';

/*
 * Request time specification.
 *
 * This class represents the begin, end and stepsize parameters.
 */
export class RequestTimeSpec {
  constructor(readonly begin?: Date, readonly end?: Date, readonly stepsize?: number) {}

  get params(): URLSearchParams {
    let params: URLSearchParams = new URLSearchParams('', new ApiQueryEncoder());
    if (this.begin != null)    params.set('begin',    this.begin.getTime().toString());
    if (this.end != null)      params.set('end',      this.end.getTime().toString());
    if (this.stepsize != null) params.set('stepsize', this.stepsize.toString());
    return params;
  }
}

/*
 * Filter time specification.
 *
 * This class is a post-processing filter argument, to synchronize the end and
 * limit the duration of a chart.
 */
export class FilterTimeSpec {
  constructor(readonly begin?: Date, readonly end?: Date, private readonly _tss?: TimeSpecService) {}

  // True iff the date is to be included.
  public test(d: Date): boolean {
    if (this.end != null && this.end.getTime() < d.getTime())
      this._tss._updateSyncEnd(d);
    return (this.begin != null ? this.begin.getTime() <= d.getTime() : true);
  }

  get active(): boolean { return this.begin != null && this.end != null; }
}

/*
 * Exception, thrown when a duration string is not parseable.
 */
export class InvalidDurationException {
  constructor(public str: string) {}
}

/** Test if the string is a valid duration. */
export function validDurationString(s: string): boolean {
  return /^\s*(?:\d+[dhms])(?:\s+\d+[dhms])+\s*$/.test(s);
}

/** Convert a number (in seconds) to a duration string. */
export function durationToString(n: number): string {
  n -= n % 1;  // Round down.
  let s: number = n % 60;
  n = (n - s) / 60;
  let m: number = n % 60;
  n = (n - m) / 60;
  let h: number = n % 24;
  n = (n - h) / 24;
  let d: number = n;

  let spec: Array<string> = new Array<string>();
  if (d !== 0) spec.push(d.toString() + 'd');
  if (h !== 0) spec.push(h.toString() + 'h');
  if (m !== 0) spec.push(m.toString() + 'm');
  if (s !== 0 || spec.length == 0) spec.push(s.toString() + 's');
  return spec.join(' ');
}

/**
 * Parse a string into a duration (in seconds).
 * Throws InvalidDurationException if the string is not a valid duration.
 */
export function durationFromString(s: string): number {
  if (!validDurationString(s))
    throw new InvalidDurationException(s);

  let result: number = 0;
  let re = /(\d+)([dhms])/g;
  for (let subspec = re.exec(s); subspec !== null; subspec = re.exec(s)) {
    let count: number = parseInt(subspec[1], 10);
    if (subspec[2] === 's') result +=                count;
    if (subspec[2] === 'm') result +=           60 * count;
    if (subspec[2] === 'h') result +=      60 * 60 * count;
    if (subspec[2] === 'd') result += 24 * 60 * 60 * count;
  }
  return result;
}

@Injectable()
export class TimeSpecService implements OnInit, OnDestroy {
  private _begin: Date = null;
  private _end: Date = null;
  private _duration: number = 3600;  // Seconds
  private _stepsize: number = null;  // Seconds
  private _syncEnd: Date = new Date();
  private _requestEv: Subject<RequestTimeSpec> = new Subject<RequestTimeSpec>();
  private _filterEv: Subject<FilterTimeSpec> = new Subject<FilterTimeSpec>();
  private _representationEv: Subject<string> = new Subject<string>();
  private _requestOut: Observable<RequestTimeSpec>;
  private _filterOut: Observable<FilterTimeSpec>;
  private _representationOut: Observable<string>;
  private _routeArgsSubscription: Subscription;

  constructor(private _router: Router, private _route: ActivatedRoute) {
    this._requestOut = this._requestEv.asObservable()
        .distinctUntilChanged()
        .share();

    this._filterOut = this._filterEv.asObservable()
        .distinctUntilChanged((a, b) => a.begin === b.begin && a.end === b.end)
        .debounceTime(50 /* ms */)  // Prevent many updates when processing tables.
        .share();

    this._representationOut = this._representationEv.asObservable()
        .distinctUntilChanged()
        .share();
  }

  ngOnInit(): void {
    this._routeArgsSubscription = this._route
        .queryParams
        .subscribe((params) => {
          let p_tsb: string = params['tsb'];
          let p_tse: string = params['tse'];
          let p_tsd: string = params['tsd'];
          let p_tss: string = params['tss'];
          if (!validDurationString(p_tsd)) p_tsd = null;  // Omit unparsable duration.
          if (!validDurationString(p_tss)) p_tss = null;  // Omit unparsable stepsize.

          // begin (tsb)
          let tsb: number = (p_tsb !== null ? parseInt(p_tsb) : null);
          // end (tse)
          let tse: number = (p_tse !== null ? parseInt(p_tse) : null);
          // duration (tsd)
          let tsd: number = (p_tsd !== null ? durationFromString(p_tsd) : null);
          // stepsize (tss)
          let tss: number = (p_tss !== null ? durationFromString(p_tss) : null);

          if (!Number.isFinite(tsb)) tsb = null;  // Omit unparsable number.
          if (!Number.isFinite(tse)) tse = null;  // Omit unparsable number.

          this._update(new Date(tsb), new Date(tse), tsd, tss);
        });
  }

  ngOnDestroy(): void {
    this._routeArgsSubscription.unsubscribe();
  }

  public update(begin?: Date, end?: Date, duration_sec?: number, stepsize_sec?: number): void {
    let params: Map<string, string> = new Map<string, string>();
    if (begin != null)        params['tsb'] = begin.getTime().toString();
    if (end != null)          params['tse'] = end.getTime().toString();
    if (duration_sec != null) params['tsd'] = durationToString(duration_sec);
    if (stepsize_sec != null) params['tss'] = durationToString(stepsize_sec);

    this._router.navigate(this._route.pathFromRoot, { skipLocationChange: true, queryParams: params });
  }

  // Change timespec.
  private _update(begin?: Date, end?: Date, duration_sec?: number, stepsize_sec?: number): void {
    this._begin = begin;
    this._end = end;
    this._duration = duration_sec;
    this._stepsize = stepsize_sec;

    this._requestEv.next(this.initialRequest);
    this._filterEv.next(this.filter);
    this._representationEv.next(this.representation);
  }

  // Inform timespec service of newly observed date.
  // Public, to allow invocation from FilterTimeSpec for updates.
  public _updateSyncEnd(d: Date): void {
    if (this._syncEnd.getTime() < d.getTime()) {
      this._syncEnd = d;
      this._filterEv.next(this.filter);
    }
  }

  get begin(): Date { return this._begin; }
  get end(): Date { return this._end; }
  get duration(): number { return this._duration; }
  get stepsize(): number { return this._stepsize; }

  get initialRequest(): RequestTimeSpec {
    let begin: Date = this._begin;
    let end: Date = this._end;
    let duration: number = (this._duration ? this._duration * 1000 : null);
    let stepsize: number = (this._stepsize ? this._stepsize * 1000 : null);

    // begin and end are both unspecified.
    if (begin == null && end == null) {
      if (duration != null)
        return new RequestTimeSpec(new Date(this._syncEnd.getTime() - duration), null, stepsize);
      else
        return new RequestTimeSpec(null, null, stepsize);
    }

    // begin is specified, end is unspecified.
    if (end == null) {
      if (duration != null)
        return new RequestTimeSpec(begin, new Date(begin.getTime() + duration), stepsize);
      else
        return new RequestTimeSpec(begin, null, stepsize);
    }

    // begin is unspecified, end is specified.
    if (begin == null) {
      if (duration == null)
        duration = 3600 * 1000;  // Provide a default, as begin=null must always have valid duration.
      return new RequestTimeSpec(new Date(end.getTime() - duration), end, stepsize);
    }

    // begin and end are both specified.
    return new RequestTimeSpec(begin, end, stepsize);
  }

  get filter(): FilterTimeSpec {
    let begin: Date = this._begin;
    let end: Date = this._end;
    let duration: number = (this._duration ? this._duration * 1000 : null);

    if (begin == null && end == null && duration == null) return new FilterTimeSpec(null, this._syncEnd, this);
    if (begin == null && end == null && duration != null) return new FilterTimeSpec(new Date(this._syncEnd.getTime() - duration), this._syncEnd, this);
    if (begin == null && end != null && duration == null) return new FilterTimeSpec();
    if (begin == null && end != null && duration != null) return new FilterTimeSpec();
    if (begin != null && end == null && duration == null) return new FilterTimeSpec(null, this._syncEnd, this);
    if (begin != null && end == null && duration != null) return new FilterTimeSpec();
    if (begin != null && end != null && duration == null) return new FilterTimeSpec();
    if (begin != null && end != null && duration != null) return new FilterTimeSpec();

    /* UNREACHABLE */
    return new FilterTimeSpec();
  }

  /** String representation of interval handled by timespec service. */
  get representation(): string {
    let begin: Date = this._begin;
    let end: Date = this._end;
    let duration: number = (this._duration ? this._duration : null);

    if (begin == null && end == null && duration == null) return 'all time';
    if (begin == null && end == null && duration != null) return durationToString(duration);
    if (begin == null && end != null && duration == null) duration = 3600;  // Handled below.
    if (begin == null && end != null && duration != null) begin = new Date(end.getTime() - duration * 1000);  // Handled below.
    if (begin != null && end == null && duration == null) return 'since ' + begin.toString();
    if (begin != null && end == null && duration != null) end = new Date(begin.getTime() + duration * 1000);  // Handled below.
    if (begin != null && end != null && duration == null) return begin.toString() + ' - ' + end.toString();
    if (begin != null && end != null && duration != null) return begin.toString() + ' - ' + end.toString();

    /* UNREACHABLE */
    return 'time spec';
  }

  get requestObservable(): Observable<RequestTimeSpec> {
    return Observable.defer(() => Observable.of(this.initialRequest).concat(this._requestOut));
  }

  get filterObservable(): Observable<FilterTimeSpec> {
    return Observable.defer(() => Observable.of(this.filter).concat(this._filterOut));
  }

  get representationObservable(): Observable<string> {
    return Observable.defer(() => Observable.of(this.representation).concat(this._representationOut));
  }
}
