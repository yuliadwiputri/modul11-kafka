# Ringkasan isi tabel raw_event (bukti hasil praktikum)
import sqlite3
 
con = sqlite3.connect('/data/raw.db')
total = con.execute('SELECT COUNT(*) FROM raw_event').fetchone()[0]
print('Total baris raw_event:', total)
print('Per cabang (kode_cabang, baris):')
sql = 'SELECT kode_cabang, COUNT(*) FROM raw_event GROUP BY 1 ORDER BY 1'
for r in con.execute(sql):
    print('  ', r)
print('Per partisi (partisi, baris):')
sql = 'SELECT partisi, COUNT(*) FROM raw_event GROUP BY 1 ORDER BY 1'
for r in con.execute(sql):
    print('  ', r)
