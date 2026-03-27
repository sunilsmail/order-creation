import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import { environment } from '../../environments/environment';

@Injectable({ providedIn: 'root' })
export class ApiService {
  constructor(private http: HttpClient) {}

  async createOrder(token: string): Promise<{ orderId: string; status: string }> {
    const headers = new HttpHeaders({ Authorization: `Bearer ${token}` });

    return firstValueFrom(
      this.http.post<{ orderId: string; status: string }>(
        `${environment.apiBaseUrl}/api/orders`,
        {
          items: [{ sku: 'BOOK-1', qty: 1 }],
          total: 100,
          location: 'US'
        },
        { headers }
      )
    );
  }
}
