import { Component }                                   from '@angular/core';
import { ChartExprFormComponent }                      from './chart-expr-form.component';
import { ChartExpr }                                   from './chart-expr';
import { OnActivate, Router, RouteSegment, RouteTree } from '@angular/router';


@Component({
  selector: 'chart-edit',
  directives: [ChartExprFormComponent],
  styles: [`
    .chart-edit {
      width: 100%;
      height: 100%;
    }
  `],
  template: `
    <h1>Monsoon Chart</h1>
    <chart-expr-form #form (expr)="onRender($event)" [initial]="exprs"></chart-expr-form>
    `
})
export class ChartEditComponent implements OnActivate {
  private exprs: Map<string, string> = new Map<string, string>();

  constructor(private router: Router) {}

  routerOnActivate(curr: RouteSegment, prev?: RouteSegment, currTree?: RouteTree, prevTree?: RouteTree): void {
    let exprs: Map<string, string> = new Map<string, string>();

    Object.keys(curr.parameters).forEach((enc_k) => {
      let k: string = decodeURIComponent(enc_k);
      let v: string = decodeURIComponent(curr.getParam(enc_k));
      if (k.startsWith("expr:")) {
        exprs[k.substr(5)] = v;
      }
    });
    this.exprs = exprs;
  }

  onRender(m: ChartExpr) {
    let args = {};
    Object.keys(m.expr).forEach((k) => {
      args[this._encode('expr:' + k)] = this._encode(m.expr[k]);
    });

    this.router.navigate(['/fe/chart-view', args]);
  }

  _encode(s: string): string {
    // Angular cannot handle round brackets in its argument. :'(
    return encodeURIComponent(s).replace(/\(/g, '%28').replace(/\)/g, '%29');
  }
}
