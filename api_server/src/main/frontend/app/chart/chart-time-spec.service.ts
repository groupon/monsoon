import { EventEmitter, Injectable, Input, Output } from '@angular/core';
import { ChartTimeSpec } from './chart-time-spec';

@Injectable()
export class ChartTimeSpecService {
  _cts = new ChartTimeSpec(3600);

  @Input()
  set time(cts: ChartTimeSpec) {
    this._cts = cts;
    this.onChange.next(this._cts);
  }
  get time() {
    return this._cts;
  }

  @Output()
  onChange: EventEmitter<ChartTimeSpec> = new EventEmitter<ChartTimeSpec>();
}
