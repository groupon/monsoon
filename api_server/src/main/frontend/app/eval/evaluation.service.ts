import { Injectable }            from '@angular/core';
import { Http, URLSearchParams } from '@angular/http';
import { Observable }            from 'rxjs/Observable';
import { Subscriber }            from 'rxjs/Subscriber';
import { ChartTimeSpecService }  from '../chart/chart-time-spec.service';
import { ChartTimeSpec }         from '../chart/chart-time-spec';
import { ApiQueryEncoder }       from '../ApiQueryEncoder';
import                                'rxjs/add/operator/concat';
import                                'rxjs/add/operator/map';
import                                'rxjs/add/operator/mergeMap';
import                                'rxjs/add/operator/switchMap';
import                                'rxjs/add/operator/do';
import                                'rxjs/add/operator/share';
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

  merge(other: EvalDataSet): EvalDataSet {
    this.lines = this.lines.concat(other.lines);
    other.headers.forEach((h) => {
      if (this.headers.indexOf(h) == -1) this.headers.push(h);
    });

    return this;
  }

  clone(): EvalDataSet {
    return new EvalDataSet(null).merge(this);
  }
}

// Evaluation service.
// This class handles the creation of evaluation iterators.
@Injectable()
export class EvaluationService {
  constructor(private http: Http, private timeSpecService: ChartTimeSpecService) {}

  evaluate(exprsObservable: Observable<Map<string, string>>): Observable<EvalDataSet> {
    console.log('EvaluationService: evaluate');
    let exprsParams = exprsObservable.map((exprs) => {
      let params: URLSearchParams = new URLSearchParams('', new ApiQueryEncoder());
      Object.keys(exprs).forEach((k) => {
        params.set('expr:' + k, exprs[k]);
      });
      return params;
    });

    return this._paramsWithTimeStream(exprsParams)
        .map((params) => new EvaluationStream(this.http, params))
        .switchMap((es) => es.begin());
  }

  // Combine query parameters with timespec information.
  private _paramsWithTimeStream(paramsObservable: Observable<URLSearchParams>): Observable<URLSearchParams> {
    return Observable.combineLatest(
        paramsObservable
            .do((p) => console.log('EvaluationService: new params ' + JSON.stringify(p))),
        Observable.of(this.timeSpecService.time)
            .concat(this.timeSpecService.onChange)
            .map(EvaluationService._timeSpecToSearchParams)
            .do((ts) => console.log('EvaluationService: new time spec ' + JSON.stringify(ts))),
        (params, tsParams) => {
          let copy: URLSearchParams = params.clone();
          copy.appendAll(tsParams)
          return copy;
        })
        .do((p) => console.log('EvaluationService: updated params ' + JSON.stringify(p)));
  }

  // Convert timespec to query parameters.
  private static _timeSpecToSearchParams(ts: ChartTimeSpec): URLSearchParams {
    let params: URLSearchParams = new URLSearchParams('', new ApiQueryEncoder());
    let end = ts.getEnd();
    let begin = ts.getBegin();
    let stepSize = ts.getStepsizeMsec();
    if (end != null) params.set('end', end.getTime().toString());
    if (begin != null) params.set('begin', begin.getTime().toString());
    if (stepSize != null) params.set('stepsize', stepSize.toString());
    return params;
  }
}

// Evaluation stream.
//
// Has an internal state and a begin() method that starts the observable.
class EvaluationStream {
  private inFlight: EvalDataSet;
  private params: URLSearchParams;
  private _out: Subscriber<EvalDataSet>;

  constructor(private http: Http, params: URLSearchParams) {
    this.inFlight = new EvalDataSet(null);
    this.params = params.clone();
    this.params.set('delay', '3000');  // Try to retrieve data every 3 seconds.
  }

  begin(): Observable<EvalDataSet> {
    console.log('EvaluationStream: begin');
    return Observable.create((inner) => {
          this._out = inner;
          this._onNext(inner);
          return () => { this._stop() };
        })
  }

  // Fetch 1 update from API iterator.
  private _onNext(inner: Observable<EvalDataSet>): void {
    this.http.get('/api/monsoon/eval', { search: this.params })
        .map((response) => response.json())
        .map((json) => new EvalIterResponse(json))
        .toPromise()  // Means we don't have to manage a subscription.
        .then(
            (resp) => this._update(inner, resp),
            (err) => {
              console.log('EvaluationStream: error ' + err.toString());
              if (this._out != null) this._out.error(err);  // Propagate error.
              this._stop();
            }
        );
  }

  // Mark output as complete.
  private _stop(): void {
    if (this._out != null) {
      console.log('ChartStream: _out.complete()');
      this._out.complete();
      this._out = null;
    }
  }

  // Process promise response from _onNext.
  private _update(inner: Observable<EvalDataSet>, resp: EvalIterResponse): void {
    // Merge with everything collected so far.
    this.inFlight.merge(resp.data);

    // Update request parameters for next request.
    this.params.set('begin', resp.newBegin.getTime().toString());
    this.params.set('cookie', resp.cookie);
    this.params.set('iter', resp.iter);

    // Only continue fetching more data if _out is still live.
    if (this._out != null) {
      // Emit copy of current inflight.
      this._out.next(this.inFlight.clone());

      if (resp.last)  // Close after last item emit.
        this._stop();
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
