import { Injectable }            from '@angular/core';
import { Http, URLSearchParams } from '@angular/http';
import { Observable }            from 'rxjs/Observable';
import { Subscriber }            from 'rxjs/Subscriber';
import { TimeSpecService,
         RequestTimeSpec,
         FilterTimeSpec }        from './time-spec';
import { ApiQueryEncoder }       from '../ApiQueryEncoder';
import                                'rxjs/add/operator/concat';
import                                'rxjs/add/operator/map';
import                                'rxjs/add/operator/mergeMap';
import                                'rxjs/add/operator/switchMap';
import                                'rxjs/add/observable/combineLatest';
import                                'rxjs/add/observable/of';

export class EvalDataSetLine {
  constructor(public date: Date, public scrape: Map<string, number>) {}
}

// Representation of an evaluation data set.
export class EvalDataSet {
  headers: Array<string>;
  lines: Array<EvalDataSetLine>;

  constructor(json: any) {
    this.headers = new Array<string>();
    this.lines = new Array<EvalDataSetLine>();

    if (json != null) {
      json.forEach((line) => {
        let date: Date = new Date(line.timestamp_msec);
        let scrape: Map<string, number> = new Map<string, number>();

        Object.keys(line.metrics).forEach((k) => {
          line.metrics[k].forEach((metric) => {
            let key: string = metric.name_tags;
            let val: number = metric.value;
            scrape[key] = val;

            if (this.headers.indexOf(key) == -1) this.headers.push(key);
          });
        });

        this.lines.push(new EvalDataSetLine(date, scrape));
      });
    }
  }

  /* Merge other data set into this. */
  merge(other: EvalDataSet): EvalDataSet {
    this.lines = this.lines.concat(other.lines);
    other.headers.forEach((h) => {
      if (this.headers.indexOf(h) == -1) this.headers.push(h);
    });

    return this;
  }

  /* Drop all scrapes before the given date. */
  retainIf(linePred: (line: EvalDataSetLine) => boolean): void {
    this.lines = this.lines.filter(linePred);

    let newKeySet: Array<string> = new Array<string>();
    this.lines.forEach((line) => {
      Object.keys(line.scrape).forEach((key) => {
        if (newKeySet.indexOf(key) == -1) newKeySet.push(key);
      });
    });
    this.headers = newKeySet;
  }

  clone(): EvalDataSet {
    return new EvalDataSet(null).merge(this);
  }
}

// Evaluation service.
// This class handles the creation of evaluation iterators.
@Injectable()
export class EvaluationService {
  constructor(private http: Http, private timeSpecService: TimeSpecService) {}

  evaluate(exprsObservable: Observable<Map<string, string>>): Observable<EvalDataSet> {
    let exprsParams = exprsObservable.map((exprs) => {
      let params: URLSearchParams = new URLSearchParams('', new ApiQueryEncoder());
      Object.keys(exprs).forEach((k) => {
        params.set('expr:' + k, exprs[k]);
      });
      return params;
    });

    return this._paramsWithTimeStream(exprsParams)
        .map((params) => new EvaluationStream(this.http, this.timeSpecService, params))
        .switchMap((es) => es.begin());
  }

  // Combine query parameters with timespec information.
  private _paramsWithTimeStream(paramsObservable: Observable<URLSearchParams>): Observable<URLSearchParams> {
    return Observable.combineLatest(
        paramsObservable,
        this.timeSpecService.requestObservable.map((r) => r.params),
        (params, tsParams) => {
          let copy: URLSearchParams = params.clone();
          copy.appendAll(tsParams)
          return copy;
        });
  }
}

// Evaluation stream.
//
// Has an internal state and a begin() method that starts the observable.
class EvaluationStream {
  private inFlight: EvalDataSet;
  private params: URLSearchParams;
  private stopped: boolean;

  constructor(private http: Http, private timeSpecService: TimeSpecService, params: URLSearchParams) {
    this.stopped = false;
    this.inFlight = new EvalDataSet(null);
    this.params = params.clone();
    this.params.set('delay', '3000');  // Try to retrieve data every 3 seconds.
  }

  begin(): Observable<EvalDataSet> {
    console.log('EvaluationStream: begin');
    let dataStream: Observable<EvalDataSet> = Observable.create((inner) => {
          this._onNext(inner);
          return () => { this._stop() };
        });
    return Observable.combineLatest(
        this.timeSpecService.filterObservable,
        dataStream,
        (filter, data) => {
          if (filter.active) data.retainIf((line) => filter.test(line.date));
          return data;
        });
  }

  // Fetch 1 update from API iterator.
  private _onNext(inner: Subscriber<EvalDataSet>): void {
    this.http.get('/api/monsoon/eval', { search: this.params })
        .map((response) => new EvalIterResponse(response.json()))
        .toPromise()  // Means we don't have to manage a subscription.
        .then(
            (resp) => this._update(inner, resp),
            (err) => {
              console.log('EvaluationStream: error ' + err.toString());
              if (!this.stopped) inner.error(err);  // Propagate error.
              inner.complete();
            }
        );
  }

  // Mark output as complete.
  private _stop(): void {
    console.log('EvaluationStream: stopped');
    this.stopped = true;
    this.inFlight = new EvalDataSet(null);  // Drop memory held by inflight data.
  }

  // Process promise response from _onNext.
  private _update(inner: Subscriber<EvalDataSet>, resp: EvalIterResponse): void {
    // Merge with everything collected so far.
    this.inFlight.merge(resp.data);

    // Filter out any scrapes that are no longer to be shown.
    let filter: FilterTimeSpec = this.timeSpecService.filter;
    if (filter.active)
      this.inFlight.retainIf((line) => filter.test(line.date));

    // Update request parameters for next request.
    this.params.set('begin', resp.newBegin.getTime().toString());
    this.params.set('cookie', resp.cookie);
    this.params.set('iter', resp.iter);

    // Only continue fetching more data if still live.
    if (!this.stopped) {
      // Emit copy of current inflight.
      inner.next(this.inFlight.clone());

      if (resp.last)  // Close after last item emit.
        inner.complete();
      else  // Continue fetching more data.
        this._onNext(inner);
    }
  }
}

// Iterator evaluation response.
class EvalIterResponse {
  cookie: string;
  iter: string;
  data: EvalDataSet;
  newBegin: Date;
  last: boolean;

  constructor(json: any) {
    this.cookie = json.cookie;
    this.iter = json.iter;
    this.data = new EvalDataSet(json.data);
    this.newBegin = new Date(json.newBegin);
    this.last = json.last;
  }
}
