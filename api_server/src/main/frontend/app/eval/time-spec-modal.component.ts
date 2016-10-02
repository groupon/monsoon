import { Component, ViewChild, Inject }     from '@angular/core'
import { FormBuilder,
         FormGroup,
         FormControl }                      from '@angular/forms'
import { TimeSpecService,
         durationToString,
         durationFromString,
         validDurationString }              from './time-spec';
import { ModalComponent }                   from 'ng2-bs3-modal/ng2-bs3-modal';
import { Router,
         ActivatedRoute }                   from '@angular/router';


function durationValidator(c: FormControl) {
  if (c.value == "") return null;
  if (validDurationString(c.value)) return null;
  return {duration: {valid: false}};
}

function dateStr(d: Date): string {
  if (d == null) return '';
  return d.toUTCString();
}

function decodeDate(s: string): Date {
  if (s == null || s === '') return null;
  return new Date(s);  // XXX Recommendation is not to use this, so find something else.
}

function durationStr(n: number): string {
  if (n == null) return '';
  return durationToString(n);
}

function decodeDuration(s: string): number {
  if (s == null || s === '') return null;
  return durationFromString(s);
}


@Component({
  selector: 'time-spec-modal',
  templateUrl: 'app/eval/time-spec-modal.html'
})
export class TimeSpecModalComponent {
  @ViewChild('dialog')
  dialog: ModalComponent;

  private begin: FormControl;
  private end: FormControl;
  private duration: FormControl;
  private stepsize: FormControl;
  private formModel: FormGroup;

  constructor(private ts: TimeSpecService, fb: FormBuilder, private _router: Router, private _route: ActivatedRoute) {
    this.begin    = fb.control('', null);  // XXX date validators
    this.end      = fb.control('', null);  // XXX date validators
    this.duration = fb.control('', durationValidator);
    this.stepsize = fb.control('', durationValidator);
    this.formModel = fb.group({
      begin: this.begin,
      end: this.end,
      duration: this.duration,
      stepsize: this.stepsize,
    });

    this._route.queryParams
        .toPromise()
        .then((params) => {
          let p_tsb: string = params['tsb'];
          let p_tse: string = params['tse'];
          let p_tsd: string = params['tsd'];
          let p_tss: string = params['tss'];
          if (!validDurationString(p_tsd)) p_tsd = null;  // Omit unparsable duration.
          if (!validDurationString(p_tss)) p_tss = null;  // Omit unparsable stepsize.

          // begin (tsb)
          let tsb: number = (p_tsb !== null ? parseInt(p_tsb) : null);
          // end (tse)
          let tse: number = (p_tse !== null ? parseInt(p_tse) : null);
          // duration (tsd)
          let tsd: number = (p_tsd !== null ? durationFromString(p_tsd) : null);
          // stepsize (tss)
          let tss: number = (p_tss !== null ? durationFromString(p_tss) : null);

          if (!Number.isFinite(tsb)) tsb = null;  // Omit unparsable number.
          if (!Number.isFinite(tse)) tse = null;  // Omit unparsable number.

          this.ts.update((tsb != null ? new Date(tsb) : null), (tss != null ? new Date(tse) : null), tsd, tss);
        });
  }

  public open(): void {
    // Initialize form with clean values.
    this.begin.setValue(dateStr(this.ts.begin));
    this.end.setValue(dateStr(this.ts.end));
    this.duration.setValue(durationStr(this.ts.duration));
    this.stepsize.setValue(durationStr(this.ts.stepsize));

    this.dialog.open();
  }

  public close(): void {
    this.dialog.close();
  }

  private onSubmit() {
    // Apply update to timespec service.
    this.ts.update(
        decodeDate(this.formModel.value.begin),
        decodeDate(this.formModel.value.end),
        decodeDuration(this.formModel.value.duration),
        decodeDuration(this.formModel.value.stepsize));

    // Apply update to URL query parameters.
    let params: Map<string, string> = new Map<string, string>();
    if (this.ts.begin != null)    params['tsb'] = this.ts.begin.getTime().toString();
    if (this.ts.end != null)      params['tse'] = this.ts.end.getTime().toString();
    if (this.ts.duration != null) params['tsd'] = durationToString(this.ts.duration);
    if (this.ts.stepsize != null) params['tss'] = durationToString(this.ts.stepsize);
    this._router.navigate([], { relativeTo: this._route, replaceUrl: true, queryParams: params });

    this.dialog.close();
  }
}
