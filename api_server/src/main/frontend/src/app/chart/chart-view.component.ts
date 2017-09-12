import { OnInit, OnDestroy, Component }      from '@angular/core';
import { ChartExpr }                         from './chart-expr';
import { ActivatedRoute, Params }            from '@angular/router';
import { ChartEditArgumentsService }         from './chart-edit-arguments.service';
import { Observable }                        from 'rxjs/Observable';
import { Subscription }                      from 'rxjs/Subscription';
import                                            'rxjs/add/operator/map';
import { encodeExpr, decodeExpr }            from './chart-edit-arguments.service';


@Component({
  selector: 'chart-view',
  template: '<chart #chart [expr]="exprs | async"></chart>'
})
export class ChartViewComponent implements OnInit, OnDestroy {
  private exprs: Observable<ChartExpr>;
  private subscription: Subscription;

  constructor(private chartEditArgumentsService: ChartEditArgumentsService, private route: ActivatedRoute) {
    this.exprs = this.route
        .params
        .map((params) => ChartViewComponent._paramsToExprMap(params))
        .map((exprs) => new ChartExpr(exprs));
  }

  ngOnInit() {
    this.subscription = this.route
        .params
        .map((params) => ChartViewComponent._paramsToExprMap(params))
        .subscribe((params) => this._exprsToChartExpr(params));
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }

  private static _paramsToExprMap(params: Params): Map<string, string> {
    let exprs = new Map<string, string>();

    Object.keys(params).forEach((enc_k) => {
      let k: string = decodeExpr(enc_k);

      if (k.startsWith("expr:")) {
        let v: string = decodeExpr(params[enc_k]);
        exprs[k.substr(5)] = v;
      }
    });
    return exprs;
  }

  _exprsToChartExpr(exprs: Map<string, string>) {
    let cea_map = new Map<string, string>();
    Object.keys(exprs).forEach((k) => {
      let v: string = exprs[k];
      cea_map['expr:' + k] = v;
    });

    this.chartEditArgumentsService.value = cea_map;
  }
}
