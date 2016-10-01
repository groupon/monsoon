import { Component,
         ElementRef,
         Input,
         Inject,
         OnInit,
         OnDestroy }                from '@angular/core';
import { ChartExpr }                from './chart-expr';
import { ChartTimeSpec }            from './chart-time-spec';
import { ChartTimeSpecService }     from './chart-time-spec.service';
import { EvaluationService,
         EvalDataSet }              from '../eval/evaluation.service';
import { Subscription }             from 'rxjs/Subscription';
import { Subject }                  from 'rxjs/Subject';
import { Observable }               from 'rxjs/Observable';
import                                   'rxjs/add/operator/debounceTime';
import                                   'rxjs/add/operator/map';
import                                   'rxjs/add/observable/defer';
import                                   'rxjs/add/observable/combineLatest';
import                                   'rxjs/add/observable/zip';
import                                   'rxjs/add/operator/do';


class ChartSize {
  constructor(public width: number, public height: number) {}
}

class ChartData {
  constructor(public table: any, public size: ChartSize) {}
}


@Component({
  selector: 'chart',
  template: 'Loading...'
})
export class ChartComponent implements OnInit, OnDestroy {
  _chart: any;
  _expr: ChartExpr;
  private subscription: Subscription;
  private chartPauser: Subject<boolean>;
  private exprSubject: Subject<ChartExpr>;
  div: any;

  @Input()
  set expr(s: ChartExpr) {
    console.log('ChartComponent: new expr ' + JSON.stringify(s));
    this._expr = s;
    this.exprSubject.next(s);
  }
  get expr() {
    return this._expr;
  }

  constructor(@Inject(ElementRef) element: ElementRef, private evalService: EvaluationService) {
    this.div = element;
    if (!(<any>window).google) { console.error("Google script not loaded."); };
    this.chartPauser = new Subject<boolean>();
    this.exprSubject = new Subject<ChartExpr>();
  }

  /* Create ChartData observable.
   *
   * We want to do this late, since:
   * - we depend on the Google Chart library to have loaded (for DataTable type)
   * - we want to use the actual size of our container element (size = 0 until rendered)
   *
   * Due to this, we miss initial events for expression updates, so we'll need
   * to replay the current value immediately.
   * We also need to replay the container size initially, since the event
   * listener will only respond to updates; it won't have an initial value.
   */
  private _createChartData(): Observable<ChartData> {
    // Start of with current value emitted, since event will only listen for resize events.
    let onResize: Observable<ChartSize> = Observable.of(new ChartSize(this.div.nativeElement.offsetWidth, this.div.nativeElement.offsetHeight))
        .concat(Observable.fromEvent(window, 'resize')
            .map(() => new ChartSize(this.div.nativeElement.offsetWidth, this.div.nativeElement.offsetHeight)))
        .debounceTime(500 /* ms */)
        .do((size) => console.log('ChartComponent: size updated ' + JSON.stringify(size)));

    // Adapt expr subject to always emit current value.
    let exprObs: Observable<Map<string, string>> = Observable.of(this._expr)
        .concat(this.exprSubject)
        .map((cexpr) => cexpr.expr)
        .do((v) => console.log('ChartComponent: requesting evaluation for ' + JSON.stringify(v)))

    // Create table stream from exprObs.
    let table: Observable<any> = this.evalService.evaluate(exprObs)
        .map(convertDataTable)
        .do((table) => console.log('ChartComponent: data updated ' + JSON.stringify(table)));

    // Combine table and size.
    let input: Observable<ChartData> = Observable.combineLatest(table, onResize, (table, size) => new ChartData(table, size));

    // Emit table and size, only once for each emitted chartPauser event.
    return Observable.zip(this.chartPauser.asObservable().do((v) => console.log('ChartComponent: chartPauser emit')), input, (_, inpVal) => inpVal)
        .do((v) => console.log('ChartComponent: emitting ChartData ' + JSON.stringify(v)));
  }

  private drawChart(table: any, size: ChartSize): void {
    if (this._chart == null) {
      console.log('ChartComponent: creating chart...');
      this._chart = new (<any>window).google.visualization.ChartWrapper({
        'chartType': 'LineChart',
        'dataTable': table,
        'options': {
          'title': 'Expression',
          'width': size.width,
          'height': size.height,
        }
      });

      (<any>window).google.visualization.events.addListener(this._chart, 'ready', () => this.chartReady());
    } else {
      console.log('ChartComponent: updating chart...');
      this._chart.setOption('width', size.width);
      this._chart.setOption('height', size.height);
      this._chart.setDataTable(table);
    }

    console.log('ChartComponent: drawing chart...');
    this._chart.draw(this.div.nativeElement);
  }

  private chartReady(): void {
    console.log('ChartComponent: chart ready for new data...');
    this.chartPauser.next(true);
  }

  private chartLibReady(): void {
    console.log('ChartComponent: chart library ready...');

    // We must subscribe before emitting anything, as items emitted
    // prior to subscription are never seen again.
    console.log('ChartComponent: creating query subscription...');
    this.subscription = this._createChartData()
        .do((v) => console.log('ChartComponent: ChartData subscription seeing ' + JSON.stringify(v)))
        .subscribe((dataAndSize) => {
          console.log('ChartComponent: ChartData = ' + JSON.stringify(dataAndSize, null, '  '));
          this.drawChart(dataAndSize.table, dataAndSize.size);
        });

    // Emit first event; must happen after subscription is created
    // or it'll be lost.
    this.chartPauser.next(true);
  }

  ngOnInit() {
    console.log('ChartComponent: loading google visualization library...');
    var self = this;
    (<any>window).google.load('visualization', '1.0', {'packages':['corechart'], callback: function() { self.chartLibReady(); }});
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }
}


function emptyDataTable(): any {
  let data: any = new (<any>window).google.visualization.DataTable();

  data.addColumn('datetime', 'Timestamp');
  data.addColumn('number', 'Placeholder');
  data.addRows(1);
  data.setCell(0, 0, new Date());
  data.setCell(0, 1, 0);
  return data;
}

function convertDataTable(eds: EvalDataSet): any {
  if (eds.lines.length == 0 || eds.headers.length == 0) return emptyDataTable();

  var data = new (<any>window).google.visualization.DataTable();

  // Create columns.
  data.addColumn('datetime', 'Timestamp');
  eds.headers.forEach((h) => {
    data.addColumn('number', h);
  });

  // Reserve space for all rows.
  data.addRows(eds.lines.length);

  // Fill all rows.
  eds.lines.forEach((line, lineNo) => {
    data.setCell(lineNo, 0, line.date);
    eds.headers.forEach((h, hIdx) => {
      data.setCell(lineNo, hIdx + 1, line.scrape[h]);
    });
  });

  return data;
}
