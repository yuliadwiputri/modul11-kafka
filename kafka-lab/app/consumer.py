# Consumer: baca event dari Kafka, simpan ke tabel raw SQLite secara idempoten
import json
import sqlite3
import sys
import time
 
from confluent_kafka import Consumer
 
BOOTSTRAP = 'kafka:9092'
TOPIC = 'transaksi'
DB = '/data/raw.db'
 
DDL = """CREATE TABLE IF NOT EXISTS raw_event (
    kode_cabang TEXT, order_id TEXT, tanggal TEXT, kode_produk TEXT,
    jumlah INTEGER, harga_satuan INTEGER,
    partisi INTEGER, offset_kafka INTEGER, diterima_pada TEXT,
    PRIMARY KEY (kode_cabang, order_id))"""
 
 
def simpan(con, e, msg):
    """Kembalikan 1 bila baris baru, 0 bila duplikat (diabaikan oleh PRIMARY KEY)."""
    cur = con.execute(
        "INSERT OR IGNORE INTO raw_event VALUES (?,?,?,?,?,?,?,?,datetime('now'))",
        (e['kode_cabang'], e['order_id'], e['tanggal'], e['kode_produk'],
         e['jumlah'], e['harga_satuan'], msg.partition(), msg.offset()))
    con.commit()
    return cur.rowcount
 
 
def main():
    nama = sys.argv[1] if len(sys.argv) > 1 else 'consumer'
    idle_maks = int(sys.argv[2]) if len(sys.argv) > 2 else 0   # 0 = tanpa batas
    group = sys.argv[3] if len(sys.argv) > 3 else 'pipeline-raw'
 
    con = sqlite3.connect(DB, timeout=30)
    con.execute(DDL)
    con.commit()
 
    c = Consumer({'bootstrap.servers': BOOTSTRAP,
                  'group.id': group,
                  'auto.offset.reset': 'earliest',
                  'enable.auto.commit': False})      # commit manual setelah disimpan
 
    def saat_assign(consumer, partisi):
        print(f'[{nama}] partisi dimiliki: {sorted(p.partition for p in partisi)}',
              flush=True)
 
    c.subscribe([TOPIC], on_assign=saat_assign)
    baru = duplikat = 0
    terakhir = time.time()
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                if idle_maks and time.time() - terakhir > idle_maks:
                    break
                continue
            if msg.error():
                print(f'[{nama}] error: {msg.error()}', flush=True)
                continue
            e = json.loads(msg.value())
            if simpan(con, e, msg):
                baru += 1
                status = 'baru'
            else:
                duplikat += 1
                status = 'duplikat diabaikan'
            print(f'[{nama}] p{msg.partition()} o{msg.offset()} '
                  f'{e["kode_cabang"]} {e["order_id"]} -> {status}', flush=True)
            c.commit(message=msg, asynchronous=False)  # commit SETELAH data tersimpan
            terakhir = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        c.close()
        con.close()
        print(f'[{nama}] selesai: baru={baru} duplikat={duplikat}', flush=True)
 
 
if __name__ == '__main__':
    main()
