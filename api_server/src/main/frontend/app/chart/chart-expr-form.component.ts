import { Component,
         EventEmitter,
         OnInit,
         Input,
         Output }                     from '@angular/core'
import { FormArray,
         FormGroup,
         FormBuilder,
         Validators }                 from '@angular/forms'
import { ChartExpr }                  from './chart-expr';
import { createExprValidator }        from './expr-validator';
import { Http }                       from '@angular/http';


class LabeledExpr {
  constructor(public label: string, public expr: string) {}
}

class Model {
  public lines: Array<LabeledExpr> = new Array();
}


@Component({
  selector: 'chart-expr-form',
  templateUrl: 'app/chart/chart-expr-form.html'
})
export class ChartExprFormComponent implements OnInit {
  formModel: FormGroup;
  lines: FormArray;

  constructor(private fb: FormBuilder, private http: Http) {
    this.lines = fb.array([]);
    this.formModel = fb.group({
      lines: this.lines
    });
  }

  @Output()
  public expr: EventEmitter<ChartExpr> = new EventEmitter<ChartExpr>();

  onSubmit() {
    // Create result from form value.
    var result = new ChartExpr(new Map<string, string>());
    var formVal = this.formModel.value;
    for (let i = 0; i < formVal.lines.length; i++) {
      result.expr[formVal.lines[i].label] = formVal.lines[i].expr;
    }
    this.expr.next(result);
  }

  ngOnInit() {
    if (this.lines.length == 0) this.add(null);
  }

  add(le: LabeledExpr) {
    let label: string = le ? le.label : '';
    let expr: string = le ? le.expr : '';
    this.lines.push(this.fb.group({
      label: this.fb.control(label, Validators.required),
      expr: this.fb.control(expr, Validators.required, createExprValidator(this.http))
    }));
  }

  @Input()
  public set initial(exprs: Map<string, string>) {
    while (this.lines.length > 0)
      this.lines.removeAt(0);

    let keys = Object.keys(exprs);
    if (keys.length == 0) this.add(null);
    keys.forEach((k) => {
      let v: string = exprs[k];
      this.add(new LabeledExpr(k, v));
    });
  }
}
