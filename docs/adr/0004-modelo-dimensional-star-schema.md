# ADR-0004: Modelo dimensional — star schema para séries econômicas BCB

## Status
Aceito

## Contexto
O bcb-warehouse precisa de um modelo dimensional que represente as três séries do Banco
Central (USD/BRL, Selic, IPCA) de forma consultável e extensível. O dbt produzirá as
tabelas finais na camada mart.

As séries têm características distintas:

| Série | Granularidade | Chave natural | Exemplo de valor |
|---|---|---|---|
| USD/BRL (código 1) | Diária | (serie_id, data) | 5.4231 (cotacao de compra) |
| Selic (código 11) | Diária | (serie_id, data) | 10.5 (% a.a.) |
| IPCA (código 433) | Mensal | (serie_id, ano, mês) | 0.44 (% no período) |

A questao central é: **como organizar essas três séries no modelo dimensional?**

Três abordagens foram consideradas:

**Abordagem A — Uma tabela fato por série** (`fct_usd_brl`, `fct_selic`, `fct_ipca`):
cada série é uma tabela independente com schema próprio.

**Abordagem B — Tabela fato única consolidada** (`fct_indicadores_economicos`) com todas
as séries em linhas, granularidade normalizada para diária (IPCA interpolado ou nulo em
dias sem medicao).

**Abordagem C — Tabela fato única com granularidade por série** (`fct_cotacoes`) sem
normalizar a granularidade — a data é o grain real de cada série.

## Decisão
**Abordagem A — Uma tabela fato por série**, complementada por dimensões compartilhadas.

Schema do mart:

```
mart/
  fct_cotacoes_diarias      -- USD/BRL e Selic (granularidade diária)
  fct_indicadores_mensais   -- IPCA (granularidade mensal)
  dim_serie                 -- metadados das séries (código BCB, nome, unidade, fonte)
  dim_data                  -- calendário (date, ano, mês, trimestre, dia_da_semana, etc.)
```

Justificativa para duas tabelas fato em vez de três: USD/BRL e Selic compartilham
exatamente a mesma granularidade (diária) e o mesmo schema de valor (`DECIMAL(18,6)`),
tornando a consolidação natural sem perda de informação. O IPCA tem granularidade
mensal e semântica diferente (variacao percentual no período vs. taxa pontual),
justificando tabela separada.

Schema detalhado de cada tabela:

### fct_cotacoes_diarias
| Coluna | Tipo | Descrição |
|---|---|---|
| cotacao_sk | VARCHAR | Surrogate key (hash de serie_id + data) |
| serie_fk | VARCHAR | FK para dim_serie |
| data_fk | DATE | FK para dim_data |
| valor | DECIMAL(18,6) | Valor da série na data |
| data_ingestao | TIMESTAMP | Quando o dado foi ingerido no pipeline |
| data_processamento | TIMESTAMP | Quando o dado foi processado pelo warehouse |

### fct_indicadores_mensais
| Coluna | Tipo | Descrição |
|---|---|---|
| indicador_sk | VARCHAR | Surrogate key (hash de serie_id + ano + mes) |
| serie_fk | VARCHAR | FK para dim_serie |
| ano | SMALLINT | Ano de referência |
| mes | TINYINT | Mês de referência (1-12) |
| valor | DECIMAL(18,6) | Valor do indicador no período |
| data_ingestao | TIMESTAMP | Quando o dado foi ingerido |
| data_processamento | TIMESTAMP | Quando o dado foi processado |

### dim_serie
| Coluna | Tipo | Descrição |
|---|---|---|
| serie_sk | VARCHAR | Surrogate key |
| serie_id | INTEGER | Código BCB da série |
| nome | VARCHAR | Nome descritivo da série |
| unidade | VARCHAR | Unidade de medida (BRL, % a.a., % período) |
| periodicidade | VARCHAR | 'diaria' ou 'mensal' |
| fonte | VARCHAR | 'BCB' |
| data_inicio_serie | DATE | Data mais antiga disponível no warehouse |

### dim_data
| Coluna | Tipo | Descrição |
|---|---|---|
| data_sk | DATE | Surrogate key (a própria data — ver nota abaixo) |
| data | DATE | Data completa |
| ano | SMALLINT | Ano |
| mes | TINYINT | Mês (1-12) |
| mes_nome | VARCHAR | Nome do mês em português |
| trimestre | TINYINT | Trimestre (1-4) |
| semestre | TINYINT | Semestre (1-2) |
| dia_da_semana | TINYINT | Dia da semana (0=domingo, 6=sábado) |
| dia_da_semana_nome | VARCHAR | Nome do dia em português |
| eh_dia_util | BOOLEAN | True se dia útil (seg-sex, excluindo feriados nacionais) |
| eh_feriado_nacional | BOOLEAN | True se feriado nacional brasileiro |

Nota sobre `data_sk`: usar a própria `DATE` como surrogate key da dimensão calendário é
prática estabelecida — evita joins desnecessários por integers arbitrários quando a data
é intrinsecamente única e human-readable.

## Consequências

### Positivas
- Modelo defensável em entrevista: star schema clássico com separacao clara entre
  fatos e dimensões
- `dim_data` com `eh_dia_util` e `eh_feriado_nacional` permite análises de janelas
  úteis (ex: Selic nos últimos 21 dias úteis) — valor analítico concreto
- Duas tabelas fato com granularidades distintas refletem a realidade dos dados
  sem forçar normalização artificial (interpolação de IPCA para diário seria enganoso)
- `dim_serie` centraliza metadados das séries — adicionar uma nova série do BCB
  no futuro requer apenas um novo registro na dimensão, sem alterar o schema das fatos
- Surrogate keys baseadas em hash permitem joins estáveis mesmo após reprocessamentos

### Negativas / Trade-offs
- `dim_data` precisa ser populada com todos os dias do intervalo histórico — modelo
  dbt seed ou tabela gerada — overhead de manutenção para feriados nacionais futuros
- Duas tabelas fato exigem que analistas conheçam qual usar dependendo da granularidade
  desejada — `fct_cotacoes_diarias` para câmbio/Selic, `fct_indicadores_mensais` para IPCA
- Surrogate keys como hash strings têm custo de join ligeiramente maior que inteiros
  sequenciais — irrelevante para o volume do BCB, mas não é o padrao ótimo em DW
  de grande escala

## Alternativas consideradas
- **Abordagem B (fato única consolidada com IPCA interpolado)**: forçar o IPCA a granularidade
  diária por interpolação ou preenchimento forward introduz dados sintéticos na camada de
  fatos, violando o princípio de que a tabela fato deve refletir eventos reais — rejeitada
- **Abordagem C (fato única sem normalizar granularidade)**: misturar dados diários e mensais
  numa única tabela com a data como grain produz um schema ambíguo onde algumas linhas
  representam dias e outras representam meses — confuso para analistas e difícil de filtrar
  corretamente — rejeitada

## Relação com outras ADRs
- ADR-0009: documenta a fonte e estratégia de população de `dim_serie` e `dim_data`
  (seed CSV) — decisão complementar a esta ADR

## Revisão
Elaborado por: Claude (Agente IA) — arquiteto-dados
Data/hora: 2026-07-18 09:00 BRT

## Aprovação
Aprovado por: Lucas de Araújo
Data/hora: 2026-07-20 12:20 BRT

## Errata — 2026-07-21

**Quatro divergências entre o schema documentado e a implementação real.**

**1. `data_sk` em `dim_data`: DATE → VARCHAR(32)**

A nota original ("usar a própria DATE como surrogate key é prática estabelecida") foi
superada durante a implementação pela decisão de usar `generate_surrogate_key` de forma
consistente em todas as dimensões (SHA-256 truncado a 32 chars hexadecimais). A macro
produz `VARCHAR(32)`, não `DATE`.

**2. `data_fk` em `fct_cotacoes_diarias`: DATE → VARCHAR(32)**

Consequência direta do ponto 1: `data_fk` referencia `dim_data.data_sk`, que é `VARCHAR(32)`.

**3. `data_ingestao` nas tabelas fato: TIMESTAMP → DATE**

A implementação usa `current_date as data_ingestao`, que produz tipo `DATE`. O schema
documentado listava `TIMESTAMP`, que seria o tipo de `current_timestamp`.

**4. `dia_da_semana` em `dim_data`: convenção `0=domingo` → `1=Segunda, 7=Domingo`**

O script `tools/generate_dim_data.py` usa `date.isoweekday()` (padrão ISO 8601), que
retorna `1=Segunda-feira` a `7=Domingo`. A ADR documentava `0=domingo, 6=sábado`
(convenção Python `weekday()`), que não corresponde à implementação.

Schema corrigido de `dim_data`:

| Coluna | Tipo | Descrição |
|---|---|---|
| data_sk | VARCHAR(32) | Surrogate key (SHA-256 truncado de `data`) |
| data | DATE | Data completa |
| ano | SMALLINT | Ano |
| mes | TINYINT | Mês (1-12) |
| mes_nome | VARCHAR | Nome do mês em português |
| trimestre | TINYINT | Trimestre (1-4) |
| semestre | TINYINT | Semestre (1-2) |
| dia_da_semana | TINYINT | Dia da semana (1=Segunda, 7=Domingo — isoweekday) |
| dia_da_semana_nome | VARCHAR | Nome do dia em português |
| eh_dia_util | BOOLEAN | True se dia útil (seg-sex, excluindo feriados nacionais) |
| eh_feriado_nacional | BOOLEAN | True se feriado nacional brasileiro |

`data_fk` em `fct_cotacoes_diarias` corrigido para `VARCHAR(32)`. Nota: `fct_indicadores_mensais` nunca teve `data_fk` — usa `ano` (INTEGER) e `mes` (INTEGER) como chave temporal por decisão de design.
`data_ingestao` em ambas as fatos corrigido para `DATE`.

**5. Macro `generate_surrogate_key`: SHA-256, não MD5**

A macro em `macros/generate_surrogate_key.sql` sobrepõe `dbt_utils.generate_surrogate_key`
usando SHA-256 truncado a 32 chars hexadecimais (128 bits efetivos), em vez do MD5 que é
o padrão do dbt_utils. Decisão: SHA-256 oferece menor probabilidade de colisão, é nativo
no Athena via `sha256() + to_hex() + to_utf8()` e não exige dependência adicional além do
próprio dbt_utils (que já é dependência do projeto). A macro é transparente ao consumidor
— o uso é idêntico: `{{ generate_surrogate_key(['campo']) }}`.

Corrigido por: Claude (Agente IA) — desenvolvedor-dados
Aprovação: Lucas de Araújo — 2026-07-21
