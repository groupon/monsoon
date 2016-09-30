import { NgModule }                    from '@angular/core';
import { BrowserModule }               from '@angular/platform-browser';
import { ReactiveFormsModule,
         FormsModule }                 from '@angular/forms';
import { routing }                     from './app.routing';
import { Ng2Bs3ModalModule }           from 'ng2-bs3-modal/ng2-bs3-modal';
import { AppComponent }                from './app.component';
import { ChartEditComponent }          from './chart/chart-edit.component';
import { ChartViewComponent }          from './chart/chart-view.component';
import { ChartTimeSpecModalComponent } from './chart/chart-time-spec-modal.component';
import { ChartExprFormComponent }      from './chart/chart-expr-form.component';
import { ChartComponent }              from './chart/chart.component';
import { DashboardComponent }          from './dashboard.component';
import { ChartEditArgumentsService }   from './chart/chart-edit-arguments.service';
import { ChartTimeSpecService }        from './chart/chart-time-spec.service';
import { HttpModule }                  from '@angular/http';


@NgModule({
  imports: [
    BrowserModule,
    FormsModule,
    ReactiveFormsModule,
    routing,
    Ng2Bs3ModalModule,
    HttpModule,
  ],
  declarations: [
    ChartTimeSpecModalComponent,
    ChartExprFormComponent,
    ChartComponent,
    AppComponent,
    ChartEditComponent,
    ChartViewComponent,
    DashboardComponent,
  ],
  providers: [ ChartEditArgumentsService, ChartTimeSpecService ],
  bootstrap: [ AppComponent ]
})
export class AppModule {
}
