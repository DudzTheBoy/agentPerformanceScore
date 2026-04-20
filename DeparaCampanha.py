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
# Enriquecer AgentPerformanceScore com de-para de campanha
# ============================

# 8a. Adicionar colunas (batch separado para evitar erro de validação)
sql_add_cols = """
IF COL_LENGTH('dbo.AgentPerformanceScore', 'Tipo') IS NULL
    ALTER TABLE dbo.AgentPerformanceScore ADD Tipo VARCHAR(200) NULL;

IF COL_LENGTH('dbo.AgentPerformanceScore', 'Operação') IS NULL
    ALTER TABLE dbo.AgentPerformanceScore ADD [Operação] VARCHAR(200) NULL;

IF COL_LENGTH('dbo.AgentPerformanceScore', 'Status') IS NULL
    ALTER TABLE dbo.AgentPerformanceScore ADD [Status] VARCHAR(200) NULL;

IF COL_LENGTH('dbo.AgentPerformanceScore', 'Ranking') IS NULL
    ALTER TABLE dbo.AgentPerformanceScore ADD Ranking VARCHAR(200) NULL;
"""

# 8b. Atualizar com base no de-para (batch separado — colunas já existem)
sql_update = """
UPDATE aps
SET aps.Tipo       = dp.Tipo,
    aps.[Operação] = dp.[Operação],
    aps.[Status]   = dp.[Status],
    aps.Ranking    = dp.Ranking
FROM dbo.AgentPerformanceScore aps
INNER JOIN dev.de_para_campanha dp
    ON dp.Campanha_id = aps.campanha_id;
"""

with engine_destino.begin() as conn_dest:
    conn_dest.execute(text(sql_add_cols))
    print("Colunas adicionadas.")

with engine_destino.begin() as conn_dest:
    conn_dest.execute(text(sql_update))
    print("De-para de campanha aplicado com sucesso.")

# Verificar quantos registros foram enriquecidos
with engine_destino.connect() as conn_dest:
    resultado = pd.read_sql("""
        SELECT 
            COUNT(*) AS total,
            SUM(CASE WHEN Tipo IS NOT NULL THEN 1 ELSE 0 END) AS com_depara
        FROM dbo.AgentPerformanceScore
    """, conn_dest)
    print(f"Total: {resultado['total'].iloc[0]:,} | Com de-para: {resultado['com_depara'].iloc[0]:,}")