import { Component, ViewChild }              from '@angular/core';
import { Router, Routes, ROUTER_DIRECTIVES } from '@angular/router';
import { ChartEditComponent }                from './chart/chart-edit.component';
import { ChartViewComponent }                from './chart/chart-view.component';
import { ChartEditArgumentsService }         from './chart/chart-edit-arguments.service';
import { ChartTimeSpecModalComponent }       from './chart/chart-time-spec-modal.component';


@Component({
  selector: 'my-app',
  directives: [ROUTER_DIRECTIVES, ChartTimeSpecModalComponent],
  providers: [ChartEditArgumentsService],
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
          <a class="navbar-brand" href="#">Mon-soon</a>
        </div>
        <div id="navbar" class="navbar-collapse collapse">
          <ul class="nav navbar-nav">
            <li><a [routerLink]="['/fe/chart-edit', chartEditArguments.value]">Edit Chart</a></li>
          </ul>
          <ul class="nav navbar-nav navbar-right">
            <li><a href="#" (click)="_chartTimeSpecModal($event)">Change Time</a></li>
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
@Routes([
  { path: '/fe/chart-edit', component: ChartEditComponent },
  { path: '/fe/chart-view', component: ChartViewComponent }
])
export class AppComponent {
  @ViewChild('chartTimeSpecModal')
  chartTimeSpecModal: ChartTimeSpecModalComponent;

  constructor(private router: Router,
              private chartEditArguments: ChartEditArgumentsService) {
  }

  _chartTimeSpecModal(event) {
    event.preventDefault();
    this.chartTimeSpecModal.open();
  }
}
