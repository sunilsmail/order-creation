import { Component } from '@angular/core';
import { Router } from '@angular/router';
import { CommonModule } from '@angular/common';
import { ApiService } from '../../services/api.service';
import { AuthService } from '../../services/auth.service';
import { SocketService } from '../../services/socket.service';

@Component({
  selector: 'app-checkout',
  standalone: true,
  imports: [CommonModule],
  template: `
    <h2>Checkout</h2>
    <p class="badge">User: {{ userId }}</p>
    <p>Create an order and subscribe to live status updates.</p>

    <button [disabled]="loading" (click)="placeOrder()">
      {{ loading ? 'Placing Order...' : 'Place Order' }}
    </button>

    <p *ngIf="error" class="status-fail">{{ error }}</p>

    <div *ngIf="lastOrderId">
      <p>Latest order: <strong>{{ lastOrderId }}</strong></p>
      <button (click)="openStatus()">Open Status Page</button>
    </div>
  `
})
export class CheckoutComponent {
  loading = false;
  error = '';
  lastOrderId = '';
  userId = '';

  constructor(
    private api: ApiService,
    private authService: AuthService,
    private socketService: SocketService,
    private router: Router
  ) {
    this.userId = this.authService.getUserId();
  }

  async placeOrder(): Promise<void> {
    this.loading = true;
    this.error = '';

    try {
      const token = await this.authService.ensureToken();
      this.socketService.connect(token);

      const order = await this.api.createOrder(token);
      this.lastOrderId = order.orderId;
      // Note: Don't join order here - Status component handles that
      // Since we immediately navigate to status page anyway

      await this.router.navigate(['/status', order.orderId]);
    } catch (error: any) {
      this.error = error?.message || 'Failed to place order';
    } finally {
      this.loading = false;
    }
  }

  openStatus(): void {
    if (!this.lastOrderId) return;
    this.router.navigate(['/status', this.lastOrderId]);
  }
}
