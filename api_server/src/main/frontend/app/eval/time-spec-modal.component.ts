import { Component, ViewChild, Inject }     from '@angular/core'
import { FormBuilder,
         FormGroup,
         FormControl }                      from '@angular/forms'
import { TimeSpecService,
         durationToString,
         durationFromString,
         validDurationString }              from './time-spec';
import { ModalComponent }                   from 'ng2-bs3-modal/ng2-bs3-modal';


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

  constructor(private ts: TimeSpecService, fb: FormBuilder) {
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
  }

  public open(): void {
    // Initialize form with clean values.
    this.formModel.value = {
      begin: dateStr(this.ts.begin),
      end: dateStr(this.ts.end),
      duration: durationStr(this.ts.duration),
      stepsize: durationStr(this.ts.stepsize),
    };

    this.dialog.open();
  }

  public close(): void {
    this.dialog.close();
  }

  private onSubmit() {
    // Apply update.
    this.ts.update(
        decodeDate(this.formModel.value.begin),
        decodeDate(this.formModel.value.end),
        decodeDuration(this.formModel.value.duration),
        decodeDuration(this.formModel.value.stepsize));

    this.dialog.close();
  }
}
