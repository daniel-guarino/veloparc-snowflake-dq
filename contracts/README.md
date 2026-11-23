# Schemas das fontes · Case Veloparc Sales & Finance

A **Veloparc** é uma transportadora de encomendas europeia fictícia (B2B e B2C, rede de hubs, depots, ParcelShops e parceiros de última milha). JSON Schemas (draft 2020-12) das 8 fontes que alimentam a camada bronze. Todos os dados são fictícios.
Campos e estados inspirados em APIs públicas de transportadoras de encomendas, nos identificadores GS1 (GLN, SSCC), no vocabulário
EPCIS/CBV, nos objetos Salesforce e nas regras de qualidade da UPU.

Os schemas descrevem **o que cada fonte promete enviar**. Servem para duas coisas:

1. **Gerar dados válidos:** padrões, domínios (`enum`), limites e campos obrigatórios.
2. **Validar na bronze:** um registo que não passa no schema é uma anomalia (vai para quarentena).

## Ficheiros

| Schema | Fonte | Tabela bronze | Tabelas silver |
|---|---|---|---|
| `veloship_shipment.schema.json` | API VeloShip (pedido + resposta) | `RAW_VELOSHIP_SHIPMENTS` | `veloship_shipment`, `veloship_parcel` |
| `tracking_event.schema.json` | Scanners / Track&Trace | `RAW_TRACKING_EVENTS` | `tracking_event` |
| `invoice.schema.json` | Faturação | `RAW_INVOICES` | `invoice`, `invoice_line`, `invoice_surcharge` |
| `crm_object.schema.json` | CRM (Opportunity ou Lead) | `RAW_CRM_OBJECTS` | `crm_opportunity`, `crm_lead` |
| `partner_settlement.schema.json` | Parceiros de última milha | `RAW_PARTNER_SETTLEMENTS` | `partner_settlement` |
| `tariff.schema.json` | Pricing | `RAW_TARIFFS` | `tariff`, `tariff_weight_band` |
| `gl_extract_row.schema.json` | ERP (uma linha do CSV) | `RAW_GL_EXTRACT` | `gl_balance` |
| `master_data.schema.json` | Master data (customer, depot ou partner) | `RAW_MASTER_DATA` | `customer`, `depot`, `partner`, `partner_zone` |

A pasta `examples/` tem um exemplo válido por tipo de objeto (11 ficheiros), todos validados contra os schemas.

## Ordem de geração (dependências)

O JSON Schema valida um objeto de cada vez. A coerência entre fontes tem de vir do gerador, por esta ordem:

1. **Master data:** depots (GLN), parceiros e as suas zonas, clientes (com algumas versões ao longo do tempo).
2. **Tarifas:** por cliente × produto × zona, com escalões de peso contíguos e validade sem sobreposição.
3. **Oportunidades e leads:** oportunidades "Closed Won" ligadas a clientes existentes; leads "Converted"
   apontam para oportunidades existentes.
4. **Shipments VeloShip:** cliente existente, 1 a 5 volumes por expedição, um TrackID por volume.
5. **Eventos de rastreio:** uma sequência por TrackID, em depots existentes.
6. **Faturas:** uma linha por TrackID efetivamente recolhido, com o preço da tarifa e do escalão certos.
7. **Liquidações de parceiros:** por parceiro, zona e mês, coerentes com os eventos DELIVERED.
8. **Extrato do razão:** por entidade e mês, igual à soma das faturas (a menos das anomalias injetadas).

## Regras entre fontes (o que o schema não exprime)

| # | Regra | Fontes envolvidas |
|---|---|---|
| R1 | `len(ParcelData) = len(ShipmentUnit)` em cada mensagem | veloship_shipment |
| R2 | Todo o `trackId` de eventos e faturas existe num shipment | tracking_event, invoice, veloship_shipment |
| R3 | Sequência de eventos: `DATA_RECEIVED → PICKUP → HUB (1 a 3) → IN_DELIVERY → DELIVERED`, com `NOT_DELIVERED` / `PARCELSHOP` como desvios; `CANCELLED` só antes de `PICKUP` | tracking_event |
| R4 | Sem repetição de (`trackId`, `status`, `bizLocation.gln`) (regra UPU de eventos repetidos) | tracking_event |
| R5 | `recordTime >= eventTime`; atraso normal de segundos a poucos minutos | tracking_event |
| R6 | `bizLocation.gln` existe como depot; o depot da última leitura pertence à zona do destinatário | tracking_event, master_data |
| R7 | `netAmount = basePrice + soma(surcharges)`; `totals.net = soma(netAmount)`; `gross = net + vat` | invoice |
| R8 | `basePrice` = preço do escalão da tarifa válida na data de envio para o peso da encomenda; sobretaxa FUEL = `basePrice × fuelSurchargePct / 100` | invoice, tariff, veloship_shipment |
| R9 | Notas de crédito: valores negativos e `relatedInvoiceNumber` existente | invoice |
| R10 | `fxRateToEur = 1` em EUR; nas outras moedas, taxa do dia de emissão | invoice |
| R11 | `deliveredParcels` = nº de TrackIDs com `DELIVERED` do parceiro, na zona e no mês | partner_settlement, tracking_event |
| R12 | `totalCost = stops × costPerStop`; `stops <= deliveredParcels` | partner_settlement |
| R13 | Receita do razão por país e mês = soma de `netAmount × fxRateToEur` das faturas (±0,5%) | gl_extract_row, invoice |
| R14 | Escalões contíguos e preço crescente; sem sobreposição de validade por (`contactId`, `product`, `zone`) | tariff |
| R15 | Dígito de controlo GS1 válido em GLN e SSCC; prefixo do `vatNumber` = `countryCode` | master_data, veloship_shipment |
| R16 | Cada zona tem exatamente um parceiro | master_data |

## Volumes e distribuições sugeridos para a simulação

| Parâmetro | Valor sugerido |
|---|---|
| Países | PT, ES, DE, PL (moedas EUR e PLN para testar câmbio) |
| Depots | 3 hubs + 20 depots + 60 ParcelShops/cacifos por país |
| Parceiros | 1 por zona, 5 zonas por país |
| Clientes | 300 (60% B2C_SHIPPER, 30% B2B_SHIPPER, 10% MARKETPLACE); 10% com 2 versões |
| Expedições | 2 meses, ~3 000 por dia útil (fins de semana a 20%; pico de Black Friday ×2,5) |
| Volumes por expedição | 1 (70%), 2 (20%), 3 a 5 (10%) |
| Peso | log-normal, mediana ~1,5 kg, máximo 30 kg |
| Entrega | 92% em 24 h, 6% em 48 h, 2% com NOT_DELIVERED antes de entregar |
| Oportunidades / leads | 400 / 1 500; 25% dos leads convertidos |

## Anomalias a injetar (≈1 a 2% dos registos de cada tipo)

| Anomalia | Viola | Dimensão |
|---|---|---|
| GLN com 12 dígitos; estado `DELIVERD` | schema (`pattern`, `enum`) | Validade |
| Nota de crédito sem `relatedInvoiceNumber` | schema (`if/then`) | Completude |
| Mensagem VeloShip com mais volumes do que TrackIDs | R1 | Completude |
| Eventos repetidos com o mesmo trackId, estado e GLN | R4 | Unicidade |
| `CANCELLED` depois de `PICKUP`; `DELIVERED` sem `IN_DELIVERY` | R3 | Consistência |
| `recordTime` horas depois de `eventTime`; um país sem eventos num dia | R5 | Atualidade |
| Lead com país "Spain" / "España" | normalização silver | Validade |
| Encomenda faturada sem `PICKUP`; entregue sem fatura | R2 | Consistência |
| Notas de crédito com sinal positivo num país (receita 2 a 3% fora do razão) | R9, R13 | Exatidão |
| Fatura de cliente que não existe no master data | R2 (gold: membro desconhecido) | Consistência |
| Tarifas com períodos sobrepostos | R14 | Consistência |
| Liquidação com `deliveredParcels` diferente dos eventos | R11 | Exatidão |

## Validar localmente

```bash
pip install jsonschema
python - <<'EOF'
import json, pathlib
from jsonschema import Draft202012Validator, FormatChecker
schema = json.load(open("schemas/tracking_event.schema.json"))
event  = json.load(open("examples/tracking_event.json"))
errors = list(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(event))
print("válido" if not errors else [e.message for e in errors])
EOF
```
