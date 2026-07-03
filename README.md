# Real-Time Network Intrusion Detection Pipeline

## Kurulum

1. Bagimliliklari kurun:
   pip install -r requirements.txt --break-system-packages --user

2. CICIDS2017 veri setini indirin (Kaggle API token gerekli):
   cd data
   kaggle datasets download -d chethuhn/network-intrusion-dataset
   unzip network-intrusion-dataset.zip
   rm network-intrusion-dataset.zip
   cd ..

3. Veriyi temizleyin/hazirlayin:
   python3 -u scripts/prepare_data.py

   Bu, data/*.csv icindeki ham dosyalari okuyup data/clean/ altina
   temizlenmis halde kaydeder (kolon adi duzeltmeleri, label normalizasyonu).

4. Docker servislerini ayaga kaldirin:
   docker compose up -d
   docker compose ps

5. Kafka topic olusturun:
   docker exec -it kafka kafka-topics --create \
     --topic network-traffic --bootstrap-server localhost:9092 \
     --partitions 3 --replication-factor 1

6. Cassandra semasini yukleyin:
   docker cp cassandra-init/schema.cql cassandra:/schema.cql
   docker exec -it cassandra cqlsh -f /schema.cql

## Klasor yapisi

- data/          : ham CICIDS2017 CSV'leri (gitignore'da, herkes kendi indirir)
- data/clean/    : temizlenmis CSV'ler (gitignore'da, prepare_data.py ile uretilir)
- scripts/       : veri hazirlik ve yardimci scriptler
- datafusion/    : DataFusion sorgu/isleme kodu
- producer/      : Kafka producer (CSV -> stream)
- cassandra-init/: Cassandra sema (CQL) dosyalari
- docs/          : rapor, paper, mimari diyagram
