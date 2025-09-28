#!/usr/bin/env python3
"""
Etsy Transactions maillerinden ORDER_ID ve BUYER_EMAIL çıkarır.
- Gmail API (OAuth)
- Sadece geçerli (6-14 haneli) order numarası ve boş olmayan e-posta yazılır
- (order_number, buyer_email) çifti tekilleştirilir
"""

from __future__ import annotations
import base64
import csv
import os
import re
import sys
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple

from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# -------------------- AYARLAR --------------------

# Güncel filtre: Gönderen(ler) + Updates sekmesi + (süreyi istersen değiştir)
BASE_QUERY = 'from:(transaction@etsy.com OR mailer@etsy.com) category:updates '

# Hızlı test için 50 yap; tam tarama için None
MAX_MESSAGES: Optional[int] = 50

OUTPUT_CSV = "etsy_orders.csv"

# Sadece salt-okuma
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# “Send Mail” düğmesini/bağlantısını bulmak için ipuçları
SEND_MAIL_CUES = [
    "send mail", "send message", "email buyer", "contact buyer",
    "e-posta gönder", "mesaj gönder"
]

# Basit e-posta regex
EMAIL_REGEX = r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}"

# Geçerli sipariş numarası tam must be 6–14 digit
ORDER_RE = re.compile(r"\b(\d{6,14})\b")

# Etsy sistem mailleri istenmiyorsa (buyer değil) dışarıda bırak
EXCLUDE_ETSY_DOMAIN = True   # True => *@etsy.com yazılmaz


# -------------------- GMAIL / YARDIMCI --------------------

def get_service():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w', encoding='utf-8') as f:
            f.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)


def search_message_ids(service, query: str) -> Iterable[str]:
    next_page = None
    seen = 0
    while True:
        resp = service.users().messages().list(
            userId='me', q=query, pageToken=next_page, maxResults=500
        ).execute()
        for m in resp.get('messages', []):
            yield m['id']
            seen += 1
            if MAX_MESSAGES and seen >= MAX_MESSAGES:
                return
        next_page = resp.get('nextPageToken')
        if not next_page:
            break


def get_message_full(service, msg_id: str) -> dict:
    return service.users().messages().get(userId='me', id=msg_id, format='full').execute()


def decode_part_data(data_b64: str) -> str:
    return base64.urlsafe_b64decode(data_b64.encode('utf-8')).decode('utf-8', errors='replace')


def iter_payload_parts(payload: dict) -> Iterable[Tuple[str, str]]:
    """(mimeType, content) çiftleri üretir (tüm alt parçalara iner)."""
    if 'parts' in payload:
        for p in payload['parts']:
            mime = p.get('mimeType', '')
            data = p.get('body', {}).get('data')
            if data:
                yield mime, decode_part_data(data)
            for sub_mime, sub_content in iter_payload_parts(p):
                yield sub_mime, sub_content
    else:
        mime = payload.get('mimeType', '')
        data = payload.get('body', {}).get('data')
        if data:
            yield mime, decode_part_data(data)


def header_get(headers: List[Dict[str, str]], name: str) -> Optional[str]:
    name_lower = name.lower()
    for h in headers:
        if h.get('name', '').lower() == name_lower:
            return h.get('value')
    return None


# -------------------- ÇIKARIM --------------------

def valid_order_numbers_from_text(text: str) -> List[str]:
    """Metinden 6–14 haneli saf sayısal order numaralarını döndürür."""
    if not text:
        return []
    # “order id is 380...” gibi durumlarda sadece sayı dönecek şekilde sıkılaştırma
    nums = {m.group(1) for m in ORDER_RE.finditer(text)}
    return list(nums)


def extract_orders(subject: str, html: str, plain: str) -> List[str]:
    found: Set[str] = set()
    # 1) Subject içinde [ ..., Order #123456789 ] gibi tipik patern
    if subject:
        for m in re.finditer(r"Order\s*#\s*(\d{6,14})", subject, re.IGNORECASE):
            found.add(m.group(1))
        found |= set(valid_order_numbers_from_text(subject))
    # 2) HTML → text
    if html:
        text_from_html = BeautifulSoup(html, 'lxml').get_text(" ")
        found |= set(valid_order_numbers_from_text(text_from_html))
    # 3) Plain text fallback
    if plain:
        found |= set(valid_order_numbers_from_text(plain))
    return list(found)


def extract_emails_from_html(html: str) -> List[str]:
    """HTML'de 'Send Mail' buton/bağlantısı ve mailto: bağlantılarından e-postaları toplar."""
    if not html:
        return []
    soup = BeautifulSoup(html, 'lxml')
    emails: Set[str] = set()

    # cue içeren linkler
    for a in soup.find_all('a', href=True):
        text = (a.get_text() or '').strip().lower()
        if any(cue in text for cue in SEND_MAIL_CUES):
            m = re.search(r"mailto:([^\"]+)", a['href'], re.IGNORECASE)
            if m:
                cand = m.group(1)
                m2 = re.search(EMAIL_REGEX, cand, re.IGNORECASE)
                if m2:
                    emails.add(m2.group(0).lower())
            for attr in ('data-email', 'data-to', 'title', 'aria-label'):
                if a.has_attr(attr):
                    m3 = re.search(EMAIL_REGEX, a[attr], re.IGNORECASE)
                    if m3:
                        emails.add(m3.group(0).lower())

    # cue içeren button/a ve yakın çevre
    for el in soup.find_all(['a', 'button']):
        text = (el.get_text() or '').strip().lower()
        if any(cue in text for cue in SEND_MAIL_CUES):
            for attr in ('data-email', 'data-to', 'title', 'aria-label'):
                if el.has_attr(attr):
                    m = re.search(EMAIL_REGEX, el[attr], re.IGNORECASE)
                    if m:
                        emails.add(m.group(0).lower())
            neighborhood = ' '.join((el.parent.get_text(' ', strip=True) if el.parent else '').split())[:1500]
            m2 = re.search(EMAIL_REGEX, neighborhood, re.IGNORECASE)
            if m2:
                emails.add(m2.group(0).lower())

    # genel mailto fallback
    for a in soup.find_all('a', href=True):
        m = re.search(r"mailto:([^\"]+)", a['href'], re.IGNORECASE)
        if m:
            cand = m.group(1)
            m2 = re.search(EMAIL_REGEX, cand, re.IGNORECASE)
            if m2:
                emails.add(m2.group(0).lower())

    return list(emails)


def extract_emails_from_text(text: str) -> List[str]:
    if not text:
        return []
    emails = {m.group(0).lower() for m in re.finditer(EMAIL_REGEX, text, re.IGNORECASE)}
    return list(emails)


def filter_buyer_emails(emails: List[str]) -> List[str]:
    """Boşları at, gerekirse *@etsy.com adreslerini çıkar."""
    cleaned = []
    for e in emails:
        if not e:
            continue
        if EXCLUDE_ETSY_DOMAIN and e.endswith("@etsy.com"):
            continue
        cleaned.append(e)
    return list(set(cleaned))  # unique


# -------------------- MAIN --------------------

def main():
    try:
        service = get_service()
    except Exception as e:
        print("[HATA] Gmail servisine bağlanırken sorun:", e)
        print("client_secret.json ve yetkilendirme adımlarını kontrol edin.")
        sys.exit(1)

    print(f"[DEBUG] Query: {BASE_QUERY}")

    # Bilgi amaçlı: yaklaşık kaç mesaj?
    first_list = service.users().messages().list(userId='me', q=BASE_QUERY, maxResults=1).execute()
    total_est = first_list.get('resultSizeEstimate')
    if total_est is not None:
        print(f"[DEBUG] Yaklaşık {total_est} mesaj bulundu.")

    rows: List[Dict[str, str]] = []
    seen_pairs: Set[Tuple[str, str]] = set()  # (order, email) çiftine göre tekilleştirme

    processed = 0
    try:
        for msg_id in search_message_ids(service, BASE_QUERY):
            try:
                msg = get_message_full(service, msg_id)
                payload = msg.get('payload', {})
                headers = payload.get('headers', [])
                subject = header_get(headers, 'Subject') or ''
                date = header_get(headers, 'Date') or ''

                html_chunks: List[str] = []
                text_chunks: List[str] = []
                for mime, content in iter_payload_parts(payload):
                    if 'html' in mime:
                        html_chunks.append(content)
                    elif 'text' in mime:
                        text_chunks.append(content)

                html = '\n'.join(html_chunks)
                plain = '\n'.join(text_chunks)

                # Order & email listelerini topla
                orders = set(extract_orders(subject, html, plain))
                emails = set(extract_emails_from_html(html))
                if not emails:
                    emails |= set(extract_emails_from_text(plain))

                # Temizle
                orders = {o for o in orders if re.fullmatch(r"\d{6,14}", o)}
                emails = set(filter_buyer_emails(list(emails)))

                # Sadece hem order hem email varsa yaz
                for o in orders:
                    for e in emails:
                        pair = (o, e)
                        if not o or not e:
                            continue
                        if pair in seen_pairs:
                            continue
                        seen_pairs.add(pair)
                        rows.append({
                            "message_id": msg_id,
                            "date": date,
                            "subject": subject,
                            "order_number": o,
                            "buyer_email": e
                        })

                processed += 1
                if processed % 100 == 0:
                    print(f"[DEBUG] {processed} mesaj işlendi...")
                time.sleep(0.02)

            except HttpError as he:
                print(f"[Uyarı] {msg_id} okunamadı: {he}")
            except Exception as ex:
                print(f"[Uyarı] {msg_id} parse edilirken hata: {ex}")

    except HttpError as e:
        print("[HATA] Listeleme sırasında HttpError:", e)
        sys.exit(2)

    # CSV: sadece (order,email) dolu olan satırlar zaten eklendi
        fieldnames = ['order_number', 'buyer_email']
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                # Eğer hem order hem mail varsa yaz
                if row['order_number'] and row['buyer_email']:
                    writer.writerow({
                        'order_number': row['order_number'],
                        'buyer_email': row['buyer_email']
                    })

    print(f"Tamamlandı. {len(rows)} kayıt yazıldı → {OUTPUT_CSV}")


if __name__ == '__main__':
    main()
