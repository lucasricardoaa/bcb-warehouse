# ADR-0008: Manutenção de tabelas Iceberg — compactação e limpeza de snapshots

## Status
Aceito

## Contexto
O Iceberg não reescreve arquivos existentes a cada operação de escrita. Cada execução do
Glue Job ou de um modelo dbt materializado cria novos arquivos Parquet por partição. Com
execuções periódicas ao longo do tempo, o número de arquivos pequenos por partição cresce,
degradando performance de queries no Athena e aumentando custo de listagem S3.

Adicionalmente, o Iceberg mantém snapshots históricos para suportar time travel. Sem
limpeza periódica, esses snapshots acumulam indefinidamente no S3, gerando custo de
armazenamento sem valor operacional após o TTL configurado.

Duas operações são necessárias para mitigar esses efeitos:

- **OPTIMIZE:** compacta arquivos pequenos em arquivos maiores (target: 128MB–512MB)
- **VACUUM:** remove arquivos de snapshot expirados do S3

A ADR-0001 registrou esse trade-off como consequência negativa do Iceberg, mas não definiu
como, quando e onde essas operações são executadas.

## Decisão
Executar OPTIMIZE + VACUUM em todas as tabelas Iceberg do `bcb_warehouse` via **DAG
dedicada `bcb_warehouse_maintenance`**, com schedule mensal fixo no dia 15 de cada mês.

A DAG é independente da `bcb_warehouse` — não é acionada por Dataset event. Isso é
necessário porque a `bcb_warehouse` é acionada exclusivamente por eventos de segunda-feira
(via Dataset da `bcb_ingestion`), e o dia 15 nunca coincide com uma segunda-feira de forma
garantida. Uma lógica de "primeira segunda-feira após o dia 15" produziria gap zero entre
a última run de dados e a manutenção, tornando o VACUUM ineficaz (ver Análise de cenários
abaixo).

Escopo das tabelas (apenas tabelas Iceberg — views dbt não suportam VACUUM/OPTIMIZE):
- `bcb_warehouse.int_indicadores_economicos`
- `bcb_warehouse.dim_serie`, `dim_data`
- `bcb_warehouse.fct_cotacoes_diarias`, `fct_indicadores_mensais`

Estrutura da DAG de manutenção:

```
bcb_warehouse_maintenance  (schedule: 0 0 15 * *)
  iceberg_optimize
    >> iceberg_vacuum
```

Parâmetros de retenção:

| Parâmetro | Valor | Justificativa |
|---|---|---|
| TTL (`history.expire.max-snapshot-age-ms`) | 7 dias | Suficiente para detectar e reverter transformações incorretas; minimiza custo S3 |
| Frequência OPTIMIZE | Mensal (dia 15) | Volume do BCB não gera acúmulo expressivo de small files em frequência menor |
| Frequência VACUUM | Mensal (dia 15) | Remove snapshots expirados acumulados no período |
| Janela de time travel efetiva | 7 dias | Controlada pelo TTL |

SQL executado via Athena para cada tabela:

```sql
OPTIMIZE bcb_warehouse.{table} REWRITE DATA USING BIN_PACK;
VACUUM bcb_warehouse.{table};
```

## Análise de cenários — comportamento do VACUUM no dia 15

Com extrações semanais (toda segunda-feira) e TTL de 7 dias, o gap entre a última
segunda-feira antes do dia 15 e o próprio dia 15 é sempre de 1 a 6 dias — dentro da
janela do TTL. Isso significa que o snapshot mais recente antes da manutenção sempre
estará válido quando o VACUUM roda.

Em modo append do Iceberg, cada snapshot referencia todos os arquivos acumulados até
aquele momento. O snapshot da última segunda-feira (válido, < 7 dias) referencia todos
os arquivos anteriores ao OPTIMIZE, impedindo sua exclusão imediata.

**Consequência documentada:** o VACUUM de cada dia 15 deleta os arquivos que foram
substituídos pelo OPTIMIZE do mês anterior — não os do mês corrente. Há uma defasagem
de aproximadamente um mês entre o OPTIMIZE compactar um arquivo e o VACUUM deletá-lo
fisicamente.

Simulação agosto–dezembro 2026 (extrações toda segunda-feira):

| Manutenção | Última segunda antes do dia 15 | Gap | O que VACUUM deleta |
|---|---|---|---|
| 15/ago | 10/ago | 5 dias | Nada (primeira execução) |
| 15/set | 14/set | 1 dia | Arquivos pré-OPTIMIZE de agosto (f_aug3, f_aug10) |
| 15/out | 12/out | 3 dias | Arquivos pré-OPTIMIZE de setembro |
| 15/nov | 9/nov  | 6 dias | Arquivos pré-OPTIMIZE de outubro |
| 15/dez | 14/dez | 1 dia | Arquivos pré-OPTIMIZE de novembro |

Este comportamento é aceito como trade-off. A defasagem de um mês é irrelevante para
o volume do BCB e produz um efeito colateral positivo: os arquivos físicos substituídos
pelo OPTIMIZE permanecem disponíveis no S3 por aproximadamente um mês, estendendo a
janela de recuperação manual além dos 7 dias de time travel via Iceberg. Em caso de
transformação incorreta detectada após o TTL, ainda há dados físicos acessíveis para
reprocessamento direto.

## Consequências

### Positivas
- Performance de queries Athena estável — sem degradação por small files
- Custo de armazenamento S3 controlado a médio prazo
- DAG de manutenção com responsabilidade única e schedule previsível
- Defasagem de um mês gera janela de recuperação manual além do time travel formal
- Demonstra no portfólio conhecimento do ciclo de vida de tabelas Iceberg, incluindo
  a interação entre TTL, modo append e frequência de manutenção

### Negativas / Trade-offs
- Terceira DAG no projeto — adiciona superfície de monitoramento
- OPTIMIZE é operação de leitura + reescrita — custo de Athena (DML scan);
  irrelevante para o volume do BCB, mas deve ser monitorado se o volume crescer
- VACUUM remove snapshots — impossibilita time travel para períodos além do TTL de 7 dias
- Arquivos substituídos pelo OPTIMIZE do mês corrente só são deletados no ciclo seguinte

## Alternativas consideradas

**OPTIMIZE + VACUUM integrados à DAG `bcb_warehouse` com short-circuit mensal**
A task verificaria via Airflow Variable se já executou no mês corrente. Rejeitada porque
a `bcb_warehouse` é Dataset-triggered e só roda às segundas-feiras. Uma lógica de
"primeira segunda-feira ≥ dia 15" produziria gap de zero dias entre a última run de dados
e a manutenção (a manutenção rodaria na mesma segunda que gerou novos dados), tornando
o VACUUM incapaz de deletar qualquer arquivo substituído pelo OPTIMIZE.

**OPTIMIZE + VACUUM no dia 1 de cada mês**
Analisado e descartado. O gap entre a última segunda-feira e o dia 1 é sempre de 1 a 6
dias — dentro do TTL de 7 dias — produzindo o mesmo problema: o snapshot da última
segunda válido impede o VACUUM de limpar os arquivos do OPTIMIZE imediato.

**OPTIMIZE por partição + VACUUM em DAG separada 8+ dias depois (padrão TB-scale)**
Arquitetura adequada para volumes de terabytes: OPTIMIZE roda ao final de cada pipeline
run sobre apenas as partições escritas; VACUUM roda em DAG própria 8–10 dias depois,
garantindo que o snapshot pré-OPTIMIZE já expirou. Rejeitada porque o volume do BCB
não justifica a complexidade adicional: nova DAG de VACUUM, rastreamento de partições
escritas por run via XCom ou manifesto S3, geração de SQL dinâmico por partição, e
coordenação de timestamps entre DAGs via Airflow Variables.

**OPTIMIZE por partição na mesma DAG (sem separar VACUUM)**
Reduz o custo por execução do OPTIMIZE mas não resolve o problema do lag do VACUUM.
Rejeitada por complexidade sem benefício proporcional para o volume do BCB.

**Redução do TTL para menos de 7 dias**
TTL de 1–2 dias resolveria o problema do lag (o snapshot da última segunda expiraria
antes do dia 15). Rejeitado porque elimina praticamente a janela de time travel, que
é o principal valor do TTL para detecção e reversão de transformações incorretas.

**Não implementar**
Mantém o trade-off documentado na ADR-0001 sem resolução. Rejeitada porque o custo
acumulado de small files e snapshots sem limpeza é real, mesmo que lento para o volume
do BCB, e a implementação agrega valor de portfólio.

## Revisão
Elaborado por: Claude (Agente IA) — arquiteto-dados
Data/hora: 2026-07-19 09:00 BRT

## Errata — 2026-07-21

**Escopo de tabelas corrigido.**

A versão original listava `stg_usd_brl`, `stg_selic` e `stg_ipca` no escopo de manutenção.
Essas são views dbt — não tabelas Iceberg — e não suportam VACUUM nem OPTIMIZE no Athena.

Adicionada `int_indicadores_economicos`, que é uma tabela Iceberg materializada na camada
intermediate e estava incorretamente omitida da lista original.

A decisão (DAG dedicada, schedule mensal no dia 15, OPTIMIZE → VACUUM) permanece inalterada.

Corrigido por: Claude (Agente IA) — arquiteto-dados
Aprovação: Lucas de Araújo — 2026-07-21
