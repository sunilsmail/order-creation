import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { CommonModule } from '@angular/common';
import { Subscription } from 'rxjs';
import { AuthService } from '../../services/auth.service';
import { SocketService, OrderUpdate } from '../../services/socket.service';

@Component({
  selector: 'app-status',
  standalone: true,
  imports: [CommonModule, RouterLink],
  template: `
    <h2>Order Status</h2>
    <p><a routerLink="/">Back to Checkout</a></p>

    <p>Tracking order: <strong>{{ orderId }}</strong></p>

    <ul>
      <li *ngFor="let event of updates" [class.status-ok]="isSuccess(event.status)" [class.status-fail]="isFailure(event.status)">
        <strong>{{ event.status }}</strong> - {{ event.message }}
      </li>
    </ul>

    <p *ngIf="updates.length === 0">Waiting for updates...</p>
  `
})
export class StatusComponent implements OnInit, OnDestroy {
  orderId = '';
  updates: OrderUpdate[] = [];
  private sub?: Subscription;

  constructor(
    private route: ActivatedRoute,
    private authService: AuthService,
    private socketService: SocketService
  ) {}

  async ngOnInit(): Promise<void> {
    this.orderId = this.route.snapshot.paramMap.get('orderId') || '';
    const token = await this.authService.ensureToken();

    this.socketService.connect(token);
    this.socketService.joinOrder(this.orderId);

    this.sub = this.socketService.updates$.subscribe((event) => {
      if (event.orderId === this.orderId) {
        // Add event to updates if it's for this order
        this.updates = [...this.updates, event];
        console.log(`✓ Event received: ${event.status}`);
      }
    });
  }

  ngOnDestroy(): void {
    this.sub?.unsubscribe();
  }

  isSuccess(status: string): boolean {
    return ['PRODUCT_AVAILABLE', 'PRICE_CONFIRMED', 'LOCATION_VALID', 'PAYMENT_SUCCESS', 'ORDER_CREATED'].includes(status);
  }

  isFailure(status: string): boolean {
    return ['PAYMENT_FAILED', 'ORDER_FAILED', 'PRODUCT_UNAVAILABLE', 'PRICE_MISMATCH', 'LOCATION_INVALID'].includes(status);
  }
}
