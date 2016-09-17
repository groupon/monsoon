import { FormControl }                      from '@angular/forms';
import { Http, URLSearchParams }            from '@angular/http';
import 'rxjs/Rx';

export class ExprValidationResult {
  constructor(public ok: boolean,
              public parseErrors?: Array<string>,
              public normalizedQuery?: string) {}
}

function exprValidationResultFromJson(json: any): ExprValidationResult {
  if (json.ok) {
    return new ExprValidationResult((<boolean>json.ok), null, (<string>json.normalized_query));
  } else {
    return new ExprValidationResult((<boolean>json.ok), (<Array<string>>json.parse_errors));
  }
}

export function createExprValidator(http: Http) {
  return function(c: FormControl): Promise<any> {
    if (c.value == "") return Promise.resolve(null);

    let params: URLSearchParams = new URLSearchParams();
    // params.set('expr', c.value);  // For now, use manual escape to work around bug in Angular2.0.0-rc.6

    return http.get('/api/monsoon/eval/validate?expr=' + encodeURIComponent(c.value), { search: params })
        .map(response => response.json())
        .map(json => exprValidationResultFromJson(json))
        .toPromise()
        .then(evr => {
          return (evr.ok ? null : { validateExpr: { valid: false, parseErrors: evr.parseErrors }});
        });
  };
}
