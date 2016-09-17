import { Component, ViewChild, Inject }     from '@angular/core'
import { NgForm }                           from '@angular/forms'
import { ChartTimeSpecService }             from './chart-time-spec.service';
import { ChartTimeSpec }                    from './chart-time-spec';
import { ModalComponent }                   from 'ng2-bs3-modal/ng2-bs3-modal';

@Component({
  selector: 'chart-time-spec-modal',
  templateUrl: 'app/chart/chart-time-spec-modal.html'
})
export class ChartTimeSpecModalComponent {
  @ViewChild('dialog')
  dialog: ModalComponent;

  model: ChartTimeSpec;

  constructor(@Inject(ChartTimeSpecService) private ts: ChartTimeSpecService) {
    this.model = this.ts.time;
  }

  public open(): void {
    this.dialog.open();
  }

  public close(): void {
    this.dialog.close();
  }

  private onSubmit() {
    this.ts.time = this.model;
    this.dialog.close();
  }
}
