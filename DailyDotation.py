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
# 3. Engine de origem (mesma base INFO_CENTRAL)
# ============================
engine_origem = engine_destino

# ============================
# 4. Query de extração (hierarquia recursiva)
# ============================
sql_select = """
;WITH hierarchy AS (
    SELECT 
        a.collaborator_id AS origin_id,
        a.reference_date,
        a.operation_id,
        a.dotation_status_id,
        a.position AS dotation_position,
        col.id,
        col.name,
        col.network_login,
        col.position AS hierarchy_position,
        col.current_supervisor_id,
        col.current_coordinator_id,
        col.current_manager_id,
        col.current_segment_id,
        0 AS level
    FROM sgdot.daily_dotation a WITH (NOLOCK)
    INNER JOIN sgdot.collaborators col WITH (NOLOCK) 
        ON a.collaborator_id = col.id
    WHERE a.reference_date >= '2026-01-01'
      AND a.position = 'operator'

    UNION ALL

    SELECT 
        h.origin_id,
        h.reference_date,
        h.operation_id,
        h.dotation_status_id,
        h.dotation_position,
        sup.id,
        sup.name,
        sup.network_login,
        sup.position,
        sup.current_supervisor_id,
        sup.current_coordinator_id,
        sup.current_manager_id,
        h.current_segment_id,
        h.level + 1
    FROM hierarchy h
    INNER JOIN sgdot.collaborators sup WITH (NOLOCK) 
        ON sup.id = CASE 
            WHEN h.hierarchy_position = 'operator'    THEN h.current_supervisor_id
            WHEN h.hierarchy_position = 'supervisor'  THEN h.current_coordinator_id
            WHEN h.hierarchy_position = 'coordinator' THEN h.current_manager_id
        END
    WHERE h.level < 3
)
SELECT 
    h.origin_id                                                           AS collaborator_id,
    MAX(CASE WHEN h.hierarchy_position = 'operator'    THEN h.name END)         AS collaborator,
    MAX(CASE WHEN h.hierarchy_position = 'operator'    THEN h.network_login END) AS network_login,
    MAX(CASE WHEN h.hierarchy_position = 'supervisor'  THEN h.id   END)   AS supervisor_id,
    MAX(CASE WHEN h.hierarchy_position = 'supervisor'  THEN h.name END)   AS supervisor,
    MAX(CASE WHEN h.hierarchy_position = 'coordinator' THEN h.id   END)   AS coordinator_id,
    MAX(CASE WHEN h.hierarchy_position = 'coordinator' THEN h.name END)   AS coordinator,
    MAX(CASE WHEN h.hierarchy_position = 'manager'     THEN h.id   END)   AS manager_id,
    MAX(CASE WHEN h.hierarchy_position = 'manager'     THEN h.name END)   AS manager,
    h.reference_date,
    h.operation_id,
    MAX(oper.name)              AS operation,
    MAX(goper.id)               AS operation_group_id,
    MAX(goper.name)             AS operation_group,
    MAX(seg.name)               AS segment,
    MAX(h.dotation_status_id)   AS dotation_status_id,
    MAX(ds.name)                AS dotation_status,
    MAX(h.dotation_position)    AS position
FROM hierarchy h
LEFT JOIN sgdot.operations oper WITH (NOLOCK) 
    ON h.operation_id = oper.id
LEFT JOIN sgdot.operations_groups goper WITH (NOLOCK) 
    ON oper.group_id = goper.id
LEFT JOIN sgdot.segments seg WITH (NOLOCK) 
    ON seg.id = h.current_segment_id
LEFT JOIN sgdot.dotation_statuses ds WITH (NOLOCK) 
    ON ds.id = h.dotation_status_id
GROUP BY 
    h.origin_id, 
    h.reference_date, 
    h.operation_id
ORDER BY h.reference_date DESC
OPTION (MAXRECURSION 4);
"""

# ============================
# 5. Tabela de destino e configuração
# ============================
tabela_destino = "agentScoreDotation"
schema_destino = "dbo"
chunk_size = 50_000

# ============================
# 6. Limpar tabela destino antes de inserir
# ============================
with engine_destino.begin() as conn:
    conn.execute(text(
        f"IF OBJECT_ID('{schema_destino}.{tabela_destino}', 'U') IS NOT NULL "
        f"TRUNCATE TABLE {schema_destino}.{tabela_destino}"
    ))
    print(f"Tabela {schema_destino}.{tabela_destino} truncada (se existia).")

# ============================
# 7. Extrair da origem e gravar no destino
# ============================
start_time = time.time()
total_rows = 0

print("Iniciando extração da dotação diária...")

for chunk in pd.read_sql(sql_select, engine_origem, chunksize=chunk_size):
    total_rows += len(chunk)
    print(f"  Chunk: {len(chunk):,} linhas | Acumulado: {total_rows:,}")
    chunk.to_sql(
        name=tabela_destino,
        schema=schema_destino,
        con=engine_destino,
        if_exists="append",
        index=False,
        chunksize=5_000,
    )

elapsed = time.time() - start_time
print(f"\nFinalizado: {total_rows:,} linhas em {elapsed/60:.1f} minutos.")