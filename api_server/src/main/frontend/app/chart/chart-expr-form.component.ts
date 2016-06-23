import { Component, EventEmitter, OnInit, Input, Output }              from '@angular/core'
import { NgForm, ControlArray, ControlGroup, FormBuilder, Validators } from '@angular/common'
import { ChartExpr }                                                   from './chart-expr';
import { ExprValidator }                                               from './expr-validator';

class LabeledExpr {
  constructor(public label: string, public expr: string) {}
}

class Model {
  public lines: Array<LabeledExpr> = new Array();
}

@Component({
  selector: 'chart-expr-form',
  directives: [ExprValidator],
  templateUrl: 'app/chart/chart-expr-form.html'
})
export class ChartExprFormComponent implements OnInit {
  model = new Model();
  formModel: ControlGroup;

  constructor(private fb: FormBuilder) {
    this.formModel = fb.group({
      lines: fb.array([])
    });
  }

  @Output()
  public expr: EventEmitter<ChartExpr> = new EventEmitter<ChartExpr>();

  onSubmit() {
    var result = new ChartExpr(new Map<string, string>());
    for (let i = 0; i < this.model.lines.length; i++) {
      let line: LabeledExpr = this.model.lines[i];
      result.expr[line.label] = line.expr;
    }
    this.expr.next(result);
  }

  ngOnInit() {
    if (this.model.lines.length == 0) this.add(null);
  }

  add(le: LabeledExpr) {
    let label: string = le ? le.label : '';
    let expr: string = le ? le.expr : '';
    (<ControlArray>this.formModel.controls['lines']).push(this.fb.group({
      label: this.fb.control(label, Validators.required),
      expr: this.fb.control(expr, Validators.required)
    }));
    this.model.lines.push(new LabeledExpr(label, expr));
  }

  @Input()
  public set initial(exprs: Map<string, string>) {
    while ((<ControlArray>this.formModel.controls['lines']).length > 0)
      (<ControlArray>this.formModel.controls['lines']).removeAt(0);
    this.model.lines = [];

    let keys = Object.keys(exprs);
    if (keys.length == 0) this.add(null);
    keys.forEach((k) => {
      let v: string = exprs[k];
      this.add(new LabeledExpr(k, v));
    });
  }
}
