import json
import time
import atexit
from sqlalchemy import create_engine, event, text

# ============================
# 1. Configuração
# ============================
with open(rf'\\10.205.218.210\DGRedeBB\Planejamento_MIS\04_MIS\SCRIPTS_PYTHON\config\config.json', 'r') as f:
    config = json.load(f)

info_central = config['INFO_CENTRAL']
engine = create_engine(
    f"mssql+pyodbc://{info_central['usuario']}:{info_central['senha']}@{info_central['servidor']}/{info_central['banco']}?driver=ODBC+Driver+17+for+SQL+Server"
)
atexit.register(engine.dispose)

# ============================
# 2. Adicionar colunas de dotação em AgentPerformanceScore (se não existirem)
# ============================
sql_add_columns = """
IF COL_LENGTH('dbo.AgentPerformanceScore', 'sgdot_collaborator')    IS NULL ALTER TABLE dbo.AgentPerformanceScore ADD sgdot_collaborator    VARCHAR(200) NULL;
IF COL_LENGTH('dbo.AgentPerformanceScore', 'sgdot_supervisor')      IS NULL ALTER TABLE dbo.AgentPerformanceScore ADD sgdot_supervisor      VARCHAR(200) NULL;
IF COL_LENGTH('dbo.AgentPerformanceScore', 'sgdot_coordinator')     IS NULL ALTER TABLE dbo.AgentPerformanceScore ADD sgdot_coordinator     VARCHAR(200) NULL;
IF COL_LENGTH('dbo.AgentPerformanceScore', 'sgdot_manager')         IS NULL ALTER TABLE dbo.AgentPerformanceScore ADD sgdot_manager         VARCHAR(200) NULL;
IF COL_LENGTH('dbo.AgentPerformanceScore', 'sgdot_date')            IS NULL ALTER TABLE dbo.AgentPerformanceScore ADD sgdot_date            DATE         NULL;
IF COL_LENGTH('dbo.AgentPerformanceScore', 'sgdot_operation')       IS NULL ALTER TABLE dbo.AgentPerformanceScore ADD sgdot_operation       VARCHAR(200) NULL;
IF COL_LENGTH('dbo.AgentPerformanceScore', 'sgdot_operation_group') IS NULL ALTER TABLE dbo.AgentPerformanceScore ADD sgdot_operation_group VARCHAR(200) NULL;
IF COL_LENGTH('dbo.AgentPerformanceScore', 'sgdot_segment')         IS NULL ALTER TABLE dbo.AgentPerformanceScore ADD sgdot_segment         VARCHAR(200) NULL;
IF COL_LENGTH('dbo.AgentPerformanceScore', 'sgdot_dotation_status') IS NULL ALTER TABLE dbo.AgentPerformanceScore ADD sgdot_dotation_status VARCHAR(200) NULL;
IF COL_LENGTH('dbo.AgentPerformanceScore', 'sgdot_position')        IS NULL ALTER TABLE dbo.AgentPerformanceScore ADD sgdot_position        VARCHAR(100) NULL;
"""

# ============================
# 3. UPDATE: enriquecer AgentPerformanceScore com dados da dotação
#    Chave: UPPER(recurso_cod) = network_login  +  CAST(dt_atendimento AS DATE) = reference_date
# ============================
sql_update = """
UPDATE aps
SET
    aps.sgdot_collaborator    = d.collaborator,
    aps.sgdot_supervisor      = d.supervisor,
    aps.sgdot_coordinator     = d.coordinator,
    aps.sgdot_manager         = d.manager,
    aps.sgdot_date            = d.reference_date,
    aps.sgdot_operation       = d.operation,
    aps.sgdot_operation_group = d.operation_group,
    aps.sgdot_segment         = d.segment,
    aps.sgdot_dotation_status = d.dotation_status,
    aps.sgdot_position        = d.position
FROM dbo.AgentPerformanceScore aps
INNER JOIN dbo.agentScoreDotation d
    ON  UPPER(aps.recurso_cod)          = d.network_login
    AND CAST(aps.dt_atendimento AS DATE) = d.reference_date;
"""

# ============================
# 4. Execução
# ============================
start_time = time.time()

with engine.begin() as conn:
    print("Adicionando colunas (se necessário)...")
    conn.execute(text(sql_add_columns))
    print("Colunas verificadas/adicionadas.")

    print("Executando UPDATE de dotação...")
    result = conn.execute(text(sql_update))
    print(f"Linhas atualizadas: {result.rowcount:,}")

elapsed = time.time() - start_time
print(f"\nFinalizado em {elapsed:.1f} segundos.")