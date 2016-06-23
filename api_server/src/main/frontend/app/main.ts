import { bootstrap }               from '@angular/platform-browser-dynamic';
import { ROUTER_PROVIDERS }        from '@angular/router';
import { HTTP_BINDINGS }           from '@angular/http';
import { ChartTimeSpecService }    from './chart/chart-time-spec.service';

import { AppComponent }            from './app.component';

bootstrap(AppComponent, [ ROUTER_PROVIDERS, HTTP_BINDINGS, ChartTimeSpecService ]);
