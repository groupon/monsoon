import { ModuleWithProviders }               from '@angular/core';
import { Routes, RouterModule }              from '@angular/router';
import { ChartEditComponent }                from './chart/chart-edit.component';
import { ChartViewComponent }                from './chart/chart-view.component';
import { DashboardComponent }                from './dashboard.component';


const appRoutes: Routes = [
  { path: 'fe/chart-edit', component: ChartEditComponent },
  { path: 'fe/chart-view', component: ChartViewComponent },
  { path: '',              component: DashboardComponent },
];

export const routing: ModuleWithProviders = RouterModule.forRoot(appRoutes);
