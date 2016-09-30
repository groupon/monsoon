import { Injectable }            from '@angular/core';
import { Http, URLSearchParams } from '@angular/http';
import { Observable }            from 'rxjs/Observable';
import { ChartTimeSpecService }  from '../chart/chart-time-spec.service';
import { ChartTimeSpec }         from '../chart/chart-time-spec';
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
        paramsObservable,
        this.timeSpecService.onChange.map(EvaluationService._timeSpecToSearchParams),
        (params, tsParams) => {
          let copy: URLSearchParams = params.clone();
          copy.appendAll(tsParams)
          return copy;
        });
  }

  // Convert timespec to query parameters.
  private static _timeSpecToSearchParams(ts: ChartTimeSpec): URLSearchParams {
    let params: URLSearchParams = new URLSearchParams('', new ApiQueryEncoder());
    let end = ts.getEnd();
    let begin = ts.getBegin();
    if (end != null) params.set('end', end.getTime().toString());
    if (begin != null) params.set('begin', begin.getTime().toString());
    return params;
  }
}

// Evaluation stream.
//
// Has an internal state and a begin() method that starts the observable.
class EvaluationStream {
  private inFlight: EvalDataSet;

  constructor(private http: Http, private params: URLSearchParams) {
    this.inFlight = new EvalDataSet(null);
    this.params.set('delay', '3000');  // Try to retrieve data every 3 seconds.
  }

  begin(): Observable<EvalDataSet> {
    return this.http.get('/api/monsoon/eval/iter', { search: this.params })
        .map((response) => response.json())
        .map((json) => new EvalIterResponse(json))
        .flatMap((resp) => this.emitAndContinue(resp));
  }

  private emitAndContinue(resp: EvalIterResponse): Observable<EvalDataSet> {
    // Merge with everything collected so far.
    this.inFlight.merge(resp.data);

    // Create observable that'll emit the result so far.
    let result: Observable<EvalDataSet> = Observable.of(this.inFlight.clone());

    // Create continuation observable, that'll get the next result delta.
    if (!resp.last) {
      this.params.set('begin', resp.newBegin.getTime().toString());
      this.params.set('cookie', resp.cookie);
      this.params.set('iter', resp.iter);
      let next: Observable<EvalDataSet> = this.http.get('/api/monsoon/eval/iter', { search: this.params })
          .map((response) => response.json())
          .map((json) => new EvalIterResponse(json))
          .flatMap((resp) => this.emitAndContinue(resp));

      result = result.concat(next);
    }
    return result;
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
