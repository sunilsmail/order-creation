import { Component } from '@angular/core';
import { RouterLink, RouterOutlet } from '@angular/router';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [RouterOutlet, RouterLink],
  template: `
    <div class="container">
      <h1>Real-Time Order Processing</h1>
      <p><a routerLink="/">Checkout</a></p>
      <router-outlet></router-outlet>
    </div>
  `
})
export class AppComponent {}
