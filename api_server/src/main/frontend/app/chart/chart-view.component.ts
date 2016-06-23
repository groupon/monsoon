import { Component }                                   from '@angular/core';
import { ChartComponent }                              from './chart.component';
import { ChartExpr }                                   from './chart-expr';
import { OnActivate, Router, RouteSegment, RouteTree } from '@angular/router';
import { ChartEditArgumentsService }                   from './chart-edit-arguments.service';


@Component({
  selector: 'chart-view',
  directives: [ChartComponent],
  template: '<chart #chart [expr]="exprs"></chart>'
})
export class ChartViewComponent implements OnActivate {
  private exprs: ChartExpr;

  constructor(private chartEditArgumentsService: ChartEditArgumentsService) {}

  routerOnActivate(curr: RouteSegment, prev?: RouteSegment, currTree?: RouteTree, prevTree?: RouteTree): void {
    // Decode arguments.
    let exprs: Map<string, string> = new Map<string, string>();
    Object.keys(curr.parameters).forEach((enc_k) => {
      let k: string = decodeURIComponent(enc_k);
      let v: string = decodeURIComponent(curr.getParam(enc_k));
      if (k.startsWith("expr:")) {
        exprs[k.substr(5)] = v;
      }
    });
    this.exprs = new ChartExpr(exprs);

    // Publish relevant arguments, so the edit link will cause us to edit the active chart.
    let cea_map: Map<string, string> = new Map<string, string>();
    Object.keys(exprs).forEach((k) => {
      let v: string = exprs[k];
      cea_map[this._encode('expr:' + k)] = this._encode(v);
    });
    this.chartEditArgumentsService.value = cea_map;
  }

  _encode(s: string): string {
    // Angular cannot handle round brackets in its argument. :'(
    return encodeURIComponent(s).replace(/\(/g, '%28').replace(/\)/g, '%29');
  }
}
