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

  connect(token: string): void {
    if (this.socket?.connected) return;

    this.socket = io(environment.socketUrl, {
      auth: { token }
    });

    this.socket.on('order-update', (data: OrderUpdate) => {
      this.updatesSubject.next(data);
    });
  }

  joinOrder(orderId: string): void {
    this.socket?.emit('join-order', { orderId });
  }
}
