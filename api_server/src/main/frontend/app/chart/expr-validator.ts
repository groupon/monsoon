import { provide, Directive, forwardRef } from '@angular/core';
import { NG_ASYNC_VALIDATORS, Control }   from '@angular/common';
import { Http, URLSearchParams }          from '@angular/http';
import 'rxjs/Rx';

export class ExprValidationResult {
  constructor(public ok: boolean,
              public parseErrors?: Array<string>,
              public normalizedQuery?: string) {}
}

@Directive({
  selector: '[validate-expr][ngControl], [validate-expr][ngModel], [validate-expr][ngFormControl]',
  providers: [
    provide(NG_ASYNC_VALIDATORS, {
      useExisting: forwardRef(() => ExprValidator),
      multi: true
    })
  ]
})
export class ExprValidator {
  constructor(private http: Http) {}

  private static exprValidationResultFromJson(json: any): ExprValidationResult {
    if (json.ok) {
      return new ExprValidationResult((<boolean>json.ok), null, (<string>json.normalized_query));
    } else {
      return new ExprValidationResult((<boolean>json.ok), (<Array<string>>json.parse_errors));
    }
  }

  public validate(c: Control): Promise<any> {
    if (c.value == "") return Promise.resolve(null);

    let params: URLSearchParams = new URLSearchParams();
    // params.set('expr', c.value);  // For now, use manual escape to work around bug in Angular2.0.0-rc.1

    return this.http.get('/api/monsoon/eval/validate?expr=' + encodeURIComponent(c.value), { search: params })
        .map(response => response.json())
        .map(json => ExprValidator.exprValidationResultFromJson(json))
        .toPromise()  // XXX don't map to promise, Observable is fine and allow unsubscribing...
        .then(evr => {
          return (evr.ok ? null : { validateExpr: { valid: false, parseErrors: evr.parseErrors }});
        });
  }
}
