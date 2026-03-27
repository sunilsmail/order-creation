import { Routes } from '@angular/router';
import { CheckoutComponent } from './components/checkout/checkout.component';
import { StatusComponent } from './components/status/status.component';

export const appRoutes: Routes = [
  { path: '', component: CheckoutComponent },
  { path: 'status/:orderId', component: StatusComponent },
  { path: '**', redirectTo: '' }
];
