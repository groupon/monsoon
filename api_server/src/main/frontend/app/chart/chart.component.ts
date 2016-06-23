import { Component, ElementRef, Input, Inject, OnInit, OnDestroy } from '@angular/core';
import { ChartExpr }                                               from './chart-expr';
import { ChartTimeSpec }                                           from './chart-time-spec';
import { ChartTimeSpecService }                                    from './chart-time-spec.service';

@Component({
  selector: 'chart',
  template: 'Loading...'
})
export class ChartComponent implements OnInit, OnDestroy {
  _expr: ChartExpr;
  _chart: any;
  _chart_width: number;
  _chart_height: number;
  _w_resize_fn: any;
  
  w: any;
  div: any;
  cts: ChartTimeSpec;

  @Input()
  set expr(s: ChartExpr) {
    this._expr = s;
    this.draw();
  }
  get expr() {
    return this._expr;
  }

  constructor(@Inject(ElementRef) element: ElementRef, ts: ChartTimeSpecService) {
    this.w = window;
    this.div = element;
    if (!this.w.google) { console.error("Google script not loaded."); };
    this.cts = ts.time;
    ts.onChange.subscribe(time => this.updateTs(time));

    this._w_resize_fn = () => { this.onResize(); };
  }

  updateTs(cts: ChartTimeSpec) {
    this.cts = cts;
    if (this._expr) this.draw();
  }

  draw() {
    var self = this;

    this.w.google.load('visualization', '1.0', {'packages':['corechart'], callback: function() { self.drawChart(); }});
  }

  drawChart() {
    this._chart = null;
    let extra_args:string = '?begin=' + encodeURIComponent(String(this.cts.getBegin().getTime())) + '&end=' + encodeURIComponent(String(this.cts.getEnd().getTime()));
    if (this.cts.getStepsizeMsec())
      extra_args += '&stepsize=' + encodeURIComponent(String(this.cts.getStepsizeMsec()));

    let keys: Array<string> = Object.keys(this._expr.expr);
    for (let i = 0; i < keys.length; ++i) {
      let k = keys[i];
      let v = this._expr.expr[k];
      extra_args += '&' + encodeURIComponent('expr:' + k) + '=' + encodeURIComponent(v);
    }

    this._chart_width = this.div.nativeElement.offsetWidth;
    this._chart_height = this.div.nativeElement.offsetHeight;

    var wrap = new this.w.google.visualization.ChartWrapper({
      'chartType': 'LineChart',
      'dataSourceUrl': '/api/monsoon/eval/gchart' + extra_args,
      'options': {
        'title': 'Expression',
        'width': this._chart_width,
        'height': this._chart_height
      }
    });

    var self = this;
    var onReady = function() {
      self._chart = wrap;
      self.onReady();
    }
    this.w.google.visualization.events.addListener(wrap, 'ready', onReady);
    wrap.draw(this.div.nativeElement);
  }

  onReady() {
    this.onResize();
  }

  onResize() {
    if (this._chart) {
      let do_update: boolean = false;
      if (this._chart_width != this.div.nativeElement.offsetWidth) {
        this._chart_width = this.div.nativeElement.offsetWidth;
        do_update = true;
      }
      if (this._chart_height != this.div.nativeElement.offsetHeight) {
        this._chart_height = this.div.nativeElement.offsetHeight;
        do_update = true;
      }
      if (do_update) this._updateSize();
    }
  }

  _updateSize() {
    this._chart.setOption('width', this._chart_width);
    this._chart.setOption('height', this._chart_height);
    this._chart.draw(this.div.nativeElement);
    this._chart = null;
  }

  ngOnInit() {
    window.addEventListener("resize", this._w_resize_fn);
  }

  ngOnDestroy() {
    window.removeEventListener("resize", this._w_resize_fn);
  }
}
