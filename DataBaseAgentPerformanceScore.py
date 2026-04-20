import pandas as pd
from sqlalchemy import create_engine, event, text
from datetime import datetime, timedelta
import json
import time
import atexit

# ============================
# 1. Carregar o arquivo de configuração
# ============================
with open(rf'\\10.205.218.210\DGRedeBB\Planejamento_MIS\04_MIS\SCRIPTS_PYTHON\config\config.json', 'r') as f:
    config = json.load(f)

# ============================
# 2. Engine para INFO_CENTRAL (destino)
# ============================
info_central = config['INFO_CENTRAL']
engine_destino = create_engine(
    f"mssql+pyodbc://{info_central['usuario']}:{info_central['senha']}@{info_central['servidor']}/{info_central['banco']}?driver=ODBC+Driver+17+for+SQL+Server"
)
atexit.register(engine_destino.dispose)

@event.listens_for(engine_destino, "before_cursor_execute")
def fast_exec_dest(conn, cursor, statement, parameters, context, executemany):
    if executemany:
        cursor.fast_executemany = True

# ============================
# 3. Engine para BSGAG001_DB (origem)
# ============================
bsg = config['BSGAG001_DB']
engine_origem = create_engine(
    f"mssql+pyodbc://{bsg['usuario']}:{bsg['senha']}@{bsg['servidor']}/{bsg['banco']}?driver=ODBC+Driver+17+for+SQL+Server"
)
atexit.register(engine_origem.dispose)

@event.listens_for(engine_origem, "before_cursor_execute")
def fast_exec_src(conn, cursor, statement, parameters, context, executemany):
    if executemany:
        cursor.fast_executemany = True

# ============================
# 4. Definição de datas
# ============================
data_carga_inicial = '2026-01-01'
dt_inicio = data_carga_inicial
dt_fim = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

# ============================
# 5. SQL — criar temp tables + SELECT (na ORIGEM)
# ============================
sql_setup_tpl = """
CREATE TABLE #sucesso (
    classe_processo_id INT NOT NULL PRIMARY KEY
);

INSERT INTO #sucesso (classe_processo_id)
SELECT classe_processo_id
FROM classe_processo WITH (NOLOCK)
WHERE classe_processo LIKE '%- Sucesso%';

CREATE TABLE #campos (
    evento_id INT NOT NULL PRIMARY KEY,
    premio    FLOAT NULL
);

INSERT INTO #campos (evento_id, premio)
SELECT
    a.evento_id,
    TRY_CONVERT(FLOAT,
        MAX(CASE WHEN a.campo_aux_id IN (2047, 2405)
                 THEN REPLACE(a.campo_txt, ',', '.')
            END)
    )
FROM evento_aux a WITH (NOLOCK)
INNER JOIN evento b WITH (NOLOCK)
    ON a.evento_id = b.evento_id
INNER JOIN #sucesso sc
    ON sc.classe_processo_id = b.classe_processo_id
WHERE a.campo_aux_id IN (2047, 2405)
  AND b.Dt_Evento >= '{dt_inicio}'
  AND b.Dt_Evento <  '{dt_fim}'
GROUP BY a.evento_id;
"""

sql_select_tpl = """
SELECT
    atend.atendimento_id,
    atend.dt_atendimento,
    op.operacao_id,
    op.operacao,
    ori.origem_id,
    ori.origem,
    camp.campanha_id,
    camp.campanha,
    rec.recurso_id,
    rec.recurso,
    UPPER(rec.recurso_cod) AS recurso_cod,
    ev.evento_id,
    cp.classe_processo_id,
    cp.classe_processo,
    gp.grupo_processo_id,
    gp.grupo_processo,
    tp.tipo_processo_id,
    tp.tipo_processo,
    ISNULL(cpc.cpc, 0) AS cpc,
    CASE WHEN sc.classe_processo_id IS NOT NULL THEN 1 ELSE 0 END AS sucesso,
    cmp.premio,
    GETDATE() AS loadDate
FROM atendimento atend WITH (NOLOCK)
INNER JOIN evento ev WITH (NOLOCK)
    ON  ev.atendimento_id = atend.atendimento_id
    AND ev.Parceiro_id NOT IN (0,1,20,21,22,23,26,124)
INNER JOIN tipo_processo tp WITH (NOLOCK)
    ON tp.tipo_processo_id = ev.tipo_processo_id
INNER JOIN grupo_processo gp WITH (NOLOCK)
    ON  gp.grupo_processo_id = tp.grupo_processo_id
    AND gp.grupo_processo_id NOT IN (512, 513)
LEFT JOIN operacao op WITH (NOLOCK)
    ON op.operacao_id = atend.operacao_id
LEFT JOIN origem ori WITH (NOLOCK)
    ON ori.origem_id = atend.origem_id
LEFT JOIN recurso rec WITH (NOLOCK)
    ON rec.recurso_id = atend.recurso_id
LEFT JOIN campanha camp WITH (NOLOCK)
    ON camp.campanha_id = ev.campanha_id
LEFT JOIN classe_processo cp WITH (NOLOCK)
    ON cp.classe_processo_id = ev.classe_processo_id
LEFT JOIN #sucesso sc
    ON sc.classe_processo_id = ev.classe_processo_id
LEFT JOIN (
    SELECT
        tlv_registro_id,
        MAX(CASE WHEN campo_txt = 'sim' THEN 1 ELSE 0 END) AS cpc
    FROM configuracao_aux WITH (NOLOCK)
    WHERE campo_aux_id = 2126
    GROUP BY tlv_registro_id
) cpc
    ON cpc.tlv_registro_id = ev.tipo_processo_id
LEFT JOIN #campos cmp
    ON cmp.evento_id = ev.evento_id
WHERE atend.dt_atendimento >= '{dt_inicio}'
  AND atend.dt_atendimento <  '{dt_fim}';
"""

# ============================
# 6. Criar tabela no DESTINO (se não existir)
# ============================
sql_create_destino = """
IF OBJECT_ID('dbo.AgentPerformanceScore', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.AgentPerformanceScore (
        atendimento_id     INT            NOT NULL,
        dt_atendimento     DATETIME       NULL,
        operacao_id        INT            NULL,
        operacao           VARCHAR(200)   NULL,
        origem_id          INT            NULL,
        origem             VARCHAR(200)   NULL,
        campanha_id        INT            NULL,
        campanha           VARCHAR(200)   NULL,
        recurso_id         INT            NULL,
        recurso            VARCHAR(200)   NULL,
        recurso_cod        VARCHAR(100)   NULL,
        evento_id          INT            NULL,
        classe_processo_id INT            NULL,
        classe_processo    VARCHAR(200)   NULL,
        grupo_processo_id  INT            NULL,
        grupo_processo     VARCHAR(200)   NULL,
        tipo_processo_id   INT            NULL,
        tipo_processo      VARCHAR(200)   NULL,
        cpc                INT            NULL,
        sucesso            INT            NULL,
        premio             FLOAT          NULL,
        loadDate           DATETIME       NOT NULL
    );
    CREATE CLUSTERED INDEX CIX_resultado_atendimento
        ON dbo.AgentPerformanceScore (atendimento_id);
END

;WITH duplicados AS (
    SELECT
        evento_id,
        ROW_NUMBER() OVER (
            PARTITION BY evento_id
            ORDER BY loadDate DESC, atendimento_id DESC
        ) AS rn
    FROM dbo.AgentPerformanceScore
    WHERE evento_id IS NOT NULL
)
DELETE FROM duplicados
WHERE rn > 1;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'UX_AgentPerformanceScore_evento_id'
      AND object_id = OBJECT_ID('dbo.AgentPerformanceScore')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_AgentPerformanceScore_evento_id
        ON dbo.AgentPerformanceScore (evento_id)
        WHERE evento_id IS NOT NULL;
END
"""

# ============================
# 7. Execução
# ============================
chunk_size = 50_000
total_rows = 0
start_time = time.time()

# 7a. Criar tabela no destino
with engine_destino.begin() as conn_dest:
    conn_dest.execute(text(sql_create_destino))
    print("Tabela destino preparada.")

with engine_destino.connect() as conn_dest:
    resultado_periodo = conn_dest.execute(
        text(f"""
            SELECT
                CAST(
                    COALESCE(
                        MAX(CAST(dt_atendimento AS date)),
                        CAST(:data_carga_inicial AS date)
                    ) AS date
                ) AS dt_inicio,
                CAST(DATEADD(DAY, 1, CAST(GETDATE() AS date)) AS date) AS dt_fim
            FROM dbo.AgentPerformanceScore
        """),
        {"data_carga_inicial": data_carga_inicial},
    ).mappings().one()

dt_inicio = resultado_periodo["dt_inicio"].strftime('%Y-%m-%d')
dt_fim = resultado_periodo["dt_fim"].strftime('%Y-%m-%d')

if dt_inicio >= dt_fim:
    raise ValueError(f"Janela de carga invalida: {dt_inicio} ate {dt_fim}")

print(f"Período automático: {dt_inicio} até {dt_fim}")

with engine_destino.begin() as conn_dest:
    conn_dest.execute(
        text("""
            DELETE FROM dbo.AgentPerformanceScore
            WHERE dt_atendimento >= :dt_inicio
              AND dt_atendimento < :dt_fim
        """),
        {"dt_inicio": dt_inicio, "dt_fim": dt_fim},
    )
    print("Janela de reprocessamento limpa no destino.")

# Formatar SQL com as datas recalculadas
sql_setup = sql_setup_tpl.format(dt_inicio=dt_inicio, dt_fim=dt_fim)
sql_select = sql_select_tpl.format(dt_inicio=dt_inicio, dt_fim=dt_fim)

# 7b. Ler da origem (mesma raw connection para manter as temp tables)
raw_conn = engine_origem.raw_connection()
try:
    cursor = raw_conn.cursor()
    # Criar temp tables
    cursor.execute(sql_setup)
    while cursor.nextset():
        pass
    print("Temp tables criadas na origem.")

    # Ler SELECT em chunks e gravar no destino
    for chunk in pd.read_sql(sql_select, raw_conn, chunksize=chunk_size):
        chunk = chunk.drop_duplicates(subset=["evento_id"], keep="last")
        total_rows += len(chunk)
        print(f"  Chunk: {len(chunk):,} linhas | Acumulado: {total_rows:,}")
        chunk.to_sql(
            name="AgentPerformanceScore",
            schema="dbo",
            con=engine_destino,
            if_exists="append",
            index=False,
            chunksize=1_000,
        )

    # Cleanup temp tables
    cursor.execute("DROP TABLE IF EXISTS #sucesso; DROP TABLE IF EXISTS #campos;")
    raw_conn.commit()
finally:
    raw_conn.close()

elapsed = time.time() - start_time
print(f"\nFinalizado: {total_rows:,} linhas em {elapsed/60:.1f} minutos.")