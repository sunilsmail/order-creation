import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import { environment } from '../../environments/environment';

@Injectable({ providedIn: 'root' })
export class AuthService {
  private readonly userId = 'demo-user';
  private token = '';

  constructor(private http: HttpClient) {}

  async ensureToken(): Promise<string> {
    if (this.token) return this.token;

    const response = await firstValueFrom(
      this.http.get<{ token: string }>(`${environment.apiBaseUrl}/api/token/${this.userId}`)
    );
    this.token = response.token;
    return this.token;
  }

  getUserId(): string {
    return this.userId;
  }
}
