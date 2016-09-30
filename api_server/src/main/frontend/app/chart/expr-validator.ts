import { FormControl }                      from '@angular/forms';
import { Http, URLSearchParams }            from '@angular/http';
import { Observable }                       from 'rxjs/Observable';
import                                           'rxjs/add/operator/map';
import                                           'rxjs/add/operator/toPromise';
import { ApiQueryEncoder }                  from '../ApiQueryEncoder';

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

    let params: URLSearchParams = new URLSearchParams('', new ApiQueryEncoder());
    params.set('expr', c.value);

    return http.get('/api/monsoon/eval/validate', { search: params })
        .map(response => response.json())
        .map(json => exprValidationResultFromJson(json))
        .toPromise()
        .then(evr => {
          return (evr.ok ? null : { validateExpr: { valid: false, parseErrors: evr.parseErrors }});
        });
  };
}
