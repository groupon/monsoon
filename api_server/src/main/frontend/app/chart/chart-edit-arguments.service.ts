import { EventEmitter, Injectable, Input, Output } from '@angular/core';

@Injectable()
export class ChartEditArgumentsService {
  private _cur: Map<string, string> = new Map<string, string>();

  @Output('value')
  value_event : EventEmitter<Map<string, string>> = new EventEmitter<Map<string, string>>();

  @Input('value')
  set value(exprs: Map<string, string>) {
    this._cur = exprs;
    this.value_event.next(exprs);
  }
  get value(): Map<string, string> {
    return this._cur;
  }
}
