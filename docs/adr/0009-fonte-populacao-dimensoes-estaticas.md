# ADR-0009: Fonte e estratégia de população das dimensões estáticas

## Status
Aceito

## Contexto
O modelo dimensional definido na ADR-0004 inclui duas dimensões:

- **`dim_data`** — calendário 2020-2030 com atributos de dia útil e feriado nacional
- **`dim_serie`** — metadados das três séries BCB (USD/BRL, Selic, IPCA)

As tabelas fato (`fct_cotacoes_diarias`, `fct_indicadores_mensais`) têm fonte clara:
`bcb-pipeline-staging` → Glue Job → Iceberg → dbt. As dimensões, porém, têm natureza
distinta — são dados de referência, não eventos — e sua fonte de população não foi
documentada na ADR-0004.

A questão central é: **como popular `dim_serie` e `dim_data` no pipeline dbt?**

Três abordagens foram consideradas para `dim_serie`:

| Abordagem | Descrição |
|---|---|
| A — dbt seed (CSV) | Arquivo CSV versionado no repositório com os 3 registros hardcoded |
| B — Modelo derivado do staging | Modelo dbt que extrai metadados a partir dos dados da `bcb-pipeline-staging` |
| C — Tabela externa / API BCB | Consulta à API do BCB ou tabela externa no Glue para obter metadados das séries |

## Decisão

**`dim_data` — dbt seed (`dim_data_seed.csv` + `feriados_nacionais.csv`)**

Já estabelecido implicitamente pela estrutura do repositório (ADR-0007). O calendário
é gerado uma vez para o intervalo 2020-2030 e versionado como CSV. Feriados nacionais
ficam em arquivo separado (`feriados_nacionais.csv`) para facilitar atualizações pontuais
sem regenerar o calendário completo.

Escopo de feriados: apenas feriados nacionais brasileiros são cobertos. Feriados estaduais
e municipais estão fora de escopo — as séries BCB (câmbio, Selic, IPCA) são indicadores
nacionais e o calendário de dias úteis relevante para análise é o nacional.

O script de geração do calendário base será versionado em `tools/generate_dim_data.py`.
Expansão do intervalo histórico (ex: até 2035) requer reexecutar o script e substituir
o `dim_data_seed.csv`.

Implicação operacional: alterações na legislação de feriados (ex: criação de novo
feriado nacional) exigem atualização manual do `feriados_nacionais.csv` e execução
de `dbt seed --full-refresh`.

**`dim_serie` — Abordagem A: dbt seed (`dim_serie.csv`)**

Um arquivo CSV com os 3 registros das séries BCB, versionado no repositório:

| serie_id | nome | unidade | periodicidade | fonte | data_inicio_serie |
|---|---|---|---|---|---|
| 1 | Taxa de câmbio - Livre - Dólar americano (compra) | R$/US$ | Diária | Banco Central do Brasil | 1984-11-28 |
| 11 | Taxa de juros - Selic acumulada no mês | % a.a. | Diária | Banco Central do Brasil | 1986-06-04 |
| 433 | IPCA - Variação mensal | % | Mensal | Banco Central do Brasil | 1980-01-01 |

Nota sobre `data_inicio_serie`: representa a data de início histórico da série na API
do BCB, não a data mais antiga disponível no warehouse. A janela histórica efetivamente
ingerida é determinada pelo bcb-pipeline — informação operacional, não um metadado da série.

Nota sobre geração da surrogate key: o arquivo `dim_serie.csv` não contém a coluna
`serie_sk`. A surrogate key é gerada pelo modelo `dim_serie.sql` via macro
`generate_surrogate_key.sql` aplicada sobre `ref('dim_serie')` — padrão dbt de separação
entre dado bruto (seed) e lógica de modelagem (modelo).

Justificativa para seed em vez de modelo derivado:
- As séries são fixas e conhecidas — não há descoberta dinâmica de séries no pipeline
- Metadados como `nome`, `unidade` e `data_inicio_serie` não estão disponíveis nos
  arquivos Parquet da `bcb-pipeline-staging` (que contêm apenas `serie_id`, `data`, `valor`)
- Um modelo derivado do staging precisaria consultar a API do BCB ou manter um mapa
  hardcoded internamente — o seed é mais explícito e rastreável
- Adicionar uma nova série ao pipeline requer mudança coordenada em bcb-pipeline
  (nova ingestão) e em bcb-warehouse (novo registro em `dim_serie.csv` + novos modelos
  dbt + `dbt seed --full-refresh`) — o seed torna essa dependência explícita e visível
  no repositório

## Consequências

### Positivas
- Fontes de todas as tabelas do mart documentadas e rastreáveis
- `dim_serie.csv` é o contrato explícito entre bcb-pipeline e bcb-warehouse: uma nova
  série só entra no warehouse quando o seed for atualizado intencionalmente
- Seeds são simples de testar (`not_null`, `unique`, `accepted_values`) e de inspecionar
- Separação de `feriados_nacionais.csv` permite atualizações de feriados sem tocar no
  calendário base

### Negativas / Trade-offs
- Manutenção manual de feriados futuros em `feriados_nacionais.csv` — risco de desatualização
- `dim_serie` hardcoded em CSV significa que metadados incorretos (ex: nome errado de série)
  exigem correção no CSV + `dbt seed --full-refresh` + reprocessamento das tabelas que
  dependem da surrogate key

## Alternativas consideradas
- **Abordagem B (modelo derivado do staging)**: os arquivos Parquet da staging não contêm
  metadados das séries além de `serie_id` — seria necessário um mapa hardcoded dentro do
  modelo SQL, o que é equivalente ao seed mas menos explícito e sem versionamento separado —
  rejeitada
- **Abordagem C (tabela externa / API BCB)**: introduz dependência de rede no pipeline dbt
  e complexidade desnecessária para 3 séries estáticas — rejeitada

## Relação com outras ADRs
- ADR-0004: define o schema das dimensões; esta ADR define como populá-las
- ADR-0007: estrutura do repositório já prevê `seeds/` com `dim_data_seed.csv` e
  `feriados_nacionais.csv`; esta ADR formaliza a decisão e adiciona `dim_serie.csv`

## Revisão
Elaborado por: Claude (Agente IA) — claude-sonnet-4-6
Data/hora: 2026-07-20 12:11 BRT
Revisado por: Claude (Agente IA) — arquiteto-dados
Data/hora: 2026-07-20 13:20 BRT

## Aprovação
Aprovado por: Lucas de Araújo
Data/hora: 2026-07-20 12:20 BRT
Reconfirmado por: Lucas de Araújo
Data/hora: 2026-07-20 13:29 BRT

## Errata — 2026-07-21

**`dim_serie.csv` renomeado para `dim_serie_seed.csv`.**

O arquivo referenciado nesta ADR como `dim_serie.csv` foi renomeado para `dim_serie_seed.csv`
para evitar conflito de nomes no Athena entre o seed e o modelo dbt `dim_serie.sql`
(o Athena não permite VIEW e TABLE com o mesmo nome no mesmo database).

Todas as referências a `dim_serie.csv` nesta ADR devem ser lidas como `dim_serie_seed.csv`.
O modelo `dim_serie.sql` referencia o seed via `ref('dim_serie_seed')`. Ver ADR-0007 errata
para detalhes da convenção de nomenclatura adotada.

Corrigido por: Claude (Agente IA) — desenvolvedor-dados
Aprovação: Lucas de Araújo — 2026-07-21
