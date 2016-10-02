import { Component, ViewChild }              from '@angular/core';
import { ChartEditArgumentsService }         from './chart/chart-edit-arguments.service';
import { ChartTimeSpecModalComponent }       from './chart/chart-time-spec-modal.component';
import { TimeSpecService }                   from './eval/time-spec';


@Component({
  selector: 'my-app',
  styles: [`
    .my-app {
      width: 100%;
      height: 100%;
    }
    .content-fill {
      width: 100%;
      height: 100%;
      margin-top: -72px;
      padding-top: 72px;
    }
  `],
  template: `
    <nav class="navbar navbar-default">
      <div class="container-fluid">
        <div class="navbar-header">
          <a class="navbar-brand" [routerLink]="['/']">Mon-soon</a>
        </div>
        <div id="navbar" class="navbar-collapse collapse">
          <ul class="nav navbar-nav">
            <li><a [routerLink]="['/fe/chart-edit', chartEditArguments.value]">Edit Chart</a></li>
          </ul>
          <ul class="nav navbar-nav navbar-right">
            <li><a href="#" (click)="_chartTimeSpecModal($event)">{{ timeSpecService.representationObservable | async }}</a></li>
          </ul>
        </div>
      </div>
    </nav>
    <div class="content-fill">
      <router-outlet></router-outlet>
    </div>
    <chart-time-spec-modal #chartTimeSpecModal></chart-time-spec-modal>
    `
})
export class AppComponent {
  @ViewChild('chartTimeSpecModal')
  chartTimeSpecModal: ChartTimeSpecModalComponent;

  constructor(private chartEditArguments: ChartEditArgumentsService, private timeSpecService: TimeSpecService) {
  }

  _chartTimeSpecModal(event) {
    event.preventDefault();
    this.chartTimeSpecModal.open();
  }
}
