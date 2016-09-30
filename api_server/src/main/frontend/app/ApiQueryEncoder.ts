import { QueryEncoder } from '@angular/http';

export class ApiQueryEncoder extends QueryEncoder {
  encodeKey(s: string): string {
    return encodeURIComponent(s);
  }

  encodeValue(s: string): string {
    return encodeURIComponent(s);
  }
}
