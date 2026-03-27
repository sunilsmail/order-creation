import { Injectable } from '@angular/core';
import { io, Socket } from 'socket.io-client';
import { Subject } from 'rxjs';
import { environment } from '../../environments/environment';

export interface OrderUpdate {
  orderId: string;
  userId: string;
  status: string;
  message: string;
}

@Injectable({ providedIn: 'root' })
export class SocketService {
  private socket?: Socket;
  private updatesSubject = new Subject<OrderUpdate>();
  updates$ = this.updatesSubject.asObservable();
  private isListenerRegistered = false;

  connect(token: string): void {
    if (this.socket?.connected) {
      console.log('Socket already connected, skipping reconnection');
      return;
    }

    this.socket = io(environment.socketUrl, {
      auth: { token }
    });

    // Register listener only once
    if (!this.isListenerRegistered) {
      this.socket.on('order-update', (data: OrderUpdate) => {
        this.updatesSubject.next(data);
      });
      this.isListenerRegistered = true;
      console.log('Socket listener registered');
    }
  }

  joinOrder(orderId: string): void {
    this.socket?.emit('join-order', { orderId });
  }
}
