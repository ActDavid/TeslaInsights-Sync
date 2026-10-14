# TeslaInsights Sync

Skickar data från Home Assistant till TeslaInsights-backenden
(`POST /api/v1/sync/ingest`). Appen gör bara utgående HTTPS-anrop. Inga portar
öppnas och inget i Home Assistant exponeras.

## Källor

| Källa | Hur | Vad |
|---|---|---|
| TeslaMate | Read-only mot TeslaMates Postgres | bilar (VIN hashas), körningar + positioner, laddningar + laddsamples, states, aktuell status |
| Tibber | Tibbers GraphQL-API | dagens/morgondagens priser och förbrukning per timme (första körningen ~31 dagar bakåt) |
| Home Assistant | Supervisor-API:t (historik) | valfria entiteter, t.ex. Zaptecs energimätare |

Varje källa är valfri. Lämna `teslamate.host`, `tibber_token` eller
`ha_entities` tomma för att stänga av den.

## Installation (lokal app)

1. Kopiera katalogen `teslainsights_sync` till `/addons/teslainsights_sync` på
   Home Assistant, via Samba- eller SSH-appen.
2. **Inställningar → Appar → App-butik → ⋮ → Sök efter uppdateringar**. Appen
   dyker upp under *Lokala appar*. Installera den; den byggs på enheten.
3. Fyll i inställningarna (se nedan) och starta. Följ loggen under fliken *Logg*.

## Read-only-konto i TeslaMate

Kör i TeslaMates databas (t.ex. via `psql` i Postgres-appen):

```sql
CREATE ROLE ti_reader LOGIN PASSWORD 'ett-långt-slumpat-lösenord';
GRANT CONNECT ON DATABASE teslamate TO ti_reader;
GRANT USAGE ON SCHEMA public TO ti_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ti_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ti_reader;
```

`teslamate.host` är Postgres-appens värdnamn i Supervisors interna nätverk.
Det står på appens sida (t.ex. `77b2833f-timescaledb`); kör du Postgres
någon annanstans anger du dess IP.

## Inställningar

| Option | Beskrivning |
|---|---|
| `api_url` | Backendens bas-URL, t.ex. `https://api.leott.se/tesla`. Måste vara https. |
| `hmac_secret` | Samma värde som `sync_hmac_secret` i backendens config. Minst 32 tecken. |
| `teslamate.*` | Anslutning till TeslaMates Postgres med read-only-kontot. |
| `sync_interval_seconds` | Hur ofta nya data hämtas (standard 300). Under backfill körs nästa omgång direkt. |
| `position_interval_seconds` | Gallring av positioner under körning (en per N sekunder). |
| `charge_sample_interval_seconds` | Gallring av laddsamples. |
| `stale_open_hours` | En körning/laddning som varit öppen längre än så skickas ändå, så att den inte blockerar kön. |
| `tibber_token` | Personlig token från developer.tibber.com. |
| `ha_entities` | Entiteter vars historik ska skickas, t.ex. `sensor.zaptec_..._total_energy`. |
| `ha_backfill_days` | Hur långt bakåt Home Assistant-historiken hämtas (högst 30). Höjs värdet hämtas den äldre historiken i efterhand, en dag per omgång. HA sparar normalt bara 10 dagars historik (`recorder: purge_keep_days`), så för 30 dagar behöver recordern behålla minst så länge. |

## Hur leveransen fungerar

- Data läses inkrementellt med en cursor per tabell/källa. Cursorn flyttas i
  samma SQLite-transaktion som batchen läggs i en outbox (`/data/sync.db`), så
  inget hoppas över vid omstart eller strömavbrott.
- `Idempotency-Key` är `källa:typ:sha256(body)`. En omsändning efter timeout ger
  `duplicate: true` i backenden i stället för dubbellagring, och oförändrad data
  (t.ex. samma status eller samma priser) köas aldrig två gånger.
- Varje anrop signeras med ny timestamp och nonce. Nätverksfel, 401, 429 och 5xx
  försöks igen med exponentiell backoff (upp till 1 h). 400/409/413 markeras som
  döda och loggas, eftersom samma body aldrig kommer att godkännas.
- När en källa felar skickas ett felmeddelande (högst en gång i timmen per fel),
  som syns i `GET /api/v1/sync/status` som `last_error_message`.
- Ligger fler än 2000 batchar olevererade pausas insamlingen tills backenden svarar.

## Format

Varje batch:

```json
{"source":"teslamate","schema_version":"1","kind":"drives","cursor":"drives:42",
 "records":[{"type":"drive","source_id":"42","tz":"UTC","data":{...}},
            {"type":"drive_position","source_id":"981","parent_id":"42","tz":"UTC","data":{...}}]}
```

TeslaMate-rader skickas som `to_jsonb(rad)`. Tidpunkter utan tidszon är UTC
(`tz: "UTC"`). Posttyper: `car`, `drive`, `drive_position`, `charging_process`,
`charge`, `state`, `vehicle_status`, `price`, `consumption`, `ha_entity`, `ha_state`.

## Felsökning

- **`Backend refused authentication`**: fel `hmac_secret`, eller klockan skiljer
  mer än `sync_max_clock_skew_seconds` från servern.
- **`Invalid configuration`**: `api_url` måste vara https och `hmac_secret` minst 32 tecken.
- Sätt `log_level: debug` för att se varje levererad batch.
