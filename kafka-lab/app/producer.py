# Producer: kirim event transaksi cabang ke topic Kafka
import json
import sys
 
from confluent_kafka import Producer
 
BOOTSTRAP = 'kafka:9092'
TOPIC = 'transaksi'
 
# (kode_cabang, order_id, tanggal, kode_produk, jumlah, harga_satuan)
EVENTS = [
    ('B01', 'A001', '2026-10-01', 'P001', 1, 7500000),
    ('B01', 'A002', '2026-10-01', 'P002', 2, 150000),
    ('B01', 'A003', '2026-10-03', 'P002', 3, 150000),
    ('B01', 'A004', '2026-10-05', 'P004', 1, 2500000),
    ('B02', 'B2001', '2026-10-02', 'P003', 1, 300000),
    ('B02', 'B2002', '2026-10-02', 'P004', 2, 2500000),
    ('B02', 'B2003', '2026-10-04', 'P002', -1, 150000),   # jumlah negatif
    ('B02', 'B2004', '2026-10-05', 'P002', 2, 150000),
    ('B02', 'B2004', '2026-10-05', 'P002', 2, 150000),   # terkirim ganda
    ('B03', 'C3001', '2026-10-03', 'P001', 1, 7500000),
    ('B03', 'C3002', '2026-10-04', 'P003', 2, 300000),
    ('B03', 'C3003', '2026-10-06', 'P009', 1, 100000),   # produk tidak terdaftar
    ('B03', 'C3004', '2026-10-06', 'P004', 1, 2500000),
]
TAMBAHAN = [
    ('B01', 'M101', '2026-10-08', 'P002', 1, 150000),
    ('B02', 'M102', '2026-10-08', 'P003', 1, 300000),
    ('B03', 'M103', '2026-10-08', 'P002', 2, 150000),
    ('B01', 'M104', '2026-10-08', 'P004', 1, 2500000),
    ('B02', 'M105', '2026-10-08', 'P002', 3, 150000),
]
SUSULAN = [
    ('B01', 'S301', '2026-10-09', 'P003', 1, 300000),
    ('B02', 'S302', '2026-10-09', 'P004', 1, 2500000),
    ('B03', 'S303', '2026-10-09', 'P002', 2, 150000),
    ('B01', 'S304', '2026-10-09', 'P002', 1, 150000),
    ('B02', 'S305', '2026-10-09', 'P003', 2, 300000),
]
PAKET = {'awal': EVENTS, 'tambahan': TAMBAHAN, 'susulan': SUSULAN}
 
 
def lapor(err, msg):
    """Dipanggil Kafka setelah broker menjawab (delivery report)."""
    if err is not None:
        print('GAGAL kirim:', err, flush=True)
    else:
        print(f'terkirim key={msg.key().decode()} partisi={msg.partition()} '
              f'offset={msg.offset()}', flush=True)
 
 
def main():
    nama = sys.argv[1] if len(sys.argv) > 1 else 'awal'
    data = PAKET[nama]
    p = Producer({'bootstrap.servers': BOOTSTRAP,
                  'acks': 'all',
                  'enable.idempotence': True})
    for cabang, order, tgl, produk, jumlah, harga in data:
        event = {'order_id': order, 'kode_cabang': cabang, 'tanggal': tgl,
                 'kode_produk': produk, 'jumlah': jumlah, 'harga_satuan': harga}
        p.produce(TOPIC, key=cabang, value=json.dumps(event), on_delivery=lapor)
        p.poll(0)             # beri kesempatan memproses delivery report
    p.flush()                 # tunggu semua pesan terkirim
    print(f'selesai: paket {nama}, {len(data)} event', flush=True)
 
 
if __name__ == '__main__':
    main()
