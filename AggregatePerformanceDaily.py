import json
import time
import atexit
from sqlalchemy import create_engine, text

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
# 2. Criar tabela destino (se não existir)
# ============================
sql_create_table = """
IF OBJECT_ID('dbo.AgentPerformanceDaily', 'U') IS NULL
CREATE TABLE dbo.AgentPerformanceDaily (
    colaborador         VARCHAR(200)    NULL,
    rede                VARCHAR(100)    NULL,
    supervisor          VARCHAR(200)    NULL,
    coordenador         VARCHAR(200)    NULL,
    gerente             VARCHAR(200)    NULL,
    grupo_operacao      VARCHAR(200)    NULL,
    operacao_sgdot      VARCHAR(200)    NULL,
    segmento            VARCHAR(200)    NULL,
    status_dotacao      VARCHAR(200)    NULL,
    posicao             VARCHAR(100)    NULL,
    dt_atendimento      DATE            NOT NULL,
    qtd_atendimentos    INT             NOT NULL DEFAULT 0,
    qtd_eventos         INT             NOT NULL DEFAULT 0,
    qtd_cpc             INT             NOT NULL DEFAULT 0,
    qtd_sucesso         INT             NOT NULL DEFAULT 0,
    total_premio        FLOAT           NOT NULL DEFAULT 0,
    retido              FLOAT           NOT NULL DEFAULT 0,
    cross_sell          INT             NOT NULL DEFAULT 0,
    upselling           FLOAT           NOT NULL DEFAULT 0,
    mon_alto            INT             NULL,
    mon_medio           INT             NULL,
    mon_baixo           INT             NULL,
    mon_elegivel        INT             NULL,
    loadDate            DATETIME        NOT NULL DEFAULT GETDATE()
);
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME   = 'AgentPerformanceDaily'
      AND COLUMN_NAME  = 'retido'
)
    ALTER TABLE dbo.AgentPerformanceDaily ADD retido FLOAT NOT NULL DEFAULT 0;
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME   = 'AgentPerformanceDaily'
      AND COLUMN_NAME  = 'cross_sell'
)
    ALTER TABLE dbo.AgentPerformanceDaily ADD cross_sell INT NOT NULL DEFAULT 0;
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME   = 'AgentPerformanceDaily'
      AND COLUMN_NAME  = 'upselling'
)
    ALTER TABLE dbo.AgentPerformanceDaily ADD upselling FLOAT NOT NULL DEFAULT 0;
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME   = 'AgentPerformanceDaily'
      AND COLUMN_NAME  = 'mon_alto'
)
    ALTER TABLE dbo.AgentPerformanceDaily ADD mon_alto INT NULL;
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME   = 'AgentPerformanceDaily'
      AND COLUMN_NAME  = 'mon_medio'
)
    ALTER TABLE dbo.AgentPerformanceDaily ADD mon_medio INT NULL;
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME   = 'AgentPerformanceDaily'
      AND COLUMN_NAME  = 'mon_baixo'
)
    ALTER TABLE dbo.AgentPerformanceDaily ADD mon_baixo INT NULL;
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME   = 'AgentPerformanceDaily'
      AND COLUMN_NAME  = 'mon_elegivel'
)
    ALTER TABLE dbo.AgentPerformanceDaily ADD mon_elegivel INT NULL;
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME   = 'AgentPerformanceDaily'
      AND COLUMN_NAME  = 'tipo_operacao'
)
    ALTER TABLE dbo.AgentPerformanceDaily ADD tipo_operacao VARCHAR(100) NULL;
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME   = 'AgentPerformanceDaily'
      AND COLUMN_NAME  = 'dt_carga'
)
    EXEC sp_rename 'dbo.AgentPerformanceDaily.dt_carga', 'loadDate', 'COLUMN';
"""

# ============================
# 3. Merge: preenche todos os dias do calendário para cada agente,
#           mesmo os dias sem atendimento (métricas = 0)
# ============================
sql_merge = """
WITH
-- Calendário de datas: de 2026-01-01 até hoje
calendario AS (
    SELECT CAST('2026-01-01' AS DATE) AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt)
    FROM calendario
    WHERE dt < CAST(GETDATE() AS DATE)
),
-- Dados reais de produção agregados por agente/dia
origem_real AS (
    SELECT
        sgdot_collaborator      AS colaborador,
        recurso_cod             AS rede,
        sgdot_supervisor        AS supervisor,
        sgdot_coordinator       AS coordenador,
        sgdot_manager           AS gerente,
        sgdot_operation_group   AS grupo_operacao,
        sgdot_operation         AS operacao_sgdot,
        sgdot_segment           AS segmento,
        sgdot_dotation_status   AS status_dotacao,
        sgdot_position          AS posicao,
        CAST(dt_atendimento AS DATE)   AS dt_atendimento,
        COUNT(DISTINCT atendimento_id) AS qtd_atendimentos,
        COUNT(DISTINCT evento_id)      AS qtd_eventos,
        SUM(cpc)                       AS qtd_cpc,
        SUM(sucesso)                   AS qtd_sucesso,
        SUM(ISNULL(premio, 0))         AS total_premio
    FROM dbo.AgentPerformanceScore
    WHERE sgdot_collaborator IS NOT NULL
      AND CAST(dt_atendimento AS DATE) >= '2026-01-01'
    GROUP BY
        sgdot_collaborator, recurso_cod, sgdot_supervisor, sgdot_coordinator,
        sgdot_manager, sgdot_operation_group, sgdot_operation, sgdot_segment,
        sgdot_dotation_status, sgdot_position,
        CAST(dt_atendimento AS DATE)
),
-- Perfil dimensional mais recente por (colaborador, rede, operacao_sgdot)
agent_dim AS (
    SELECT
        colaborador, rede, supervisor, coordenador, gerente,
        grupo_operacao, operacao_sgdot, segmento, status_dotacao, posicao
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY colaborador, rede, operacao_sgdot
                   ORDER BY dt_atendimento DESC
               ) AS rn
        FROM origem_real
    ) t
    WHERE rn = 1
),
-- Retenção agregada por rede/data
retencao AS (
    SELECT
        REDE,
        CAST(DATA AS DATE)  AS dt_atendimento,
        SUM(RETIDO)         AS retido
    FROM [gerencial].[Base_Gerencial_Brasilseg_Retencao]
    WHERE DATA >= '2026-01-01'
    GROUP BY REDE, CAST(DATA AS DATE)
),
-- Cross-sell agregado por rede/data
cross_sell AS (
    SELECT
        rede,
        dt_atendimento,
        SUM(CASE
            WHEN PRODUTO_VENDA = 'ITENS PESSOAIS' THEN 1
            WHEN OPERACAO_DOTACAO_BASE = 'AUTO ASSESSORIA'
                 AND PRODUTO_VENDA IN ('CRÉDITO PROTEGIDO','RESIDENCIAL','VIDA','ITENS PESSOAIS','PATRIMONIAL','EMPRESARIAL') THEN 1
            WHEN OPERACAO_DOTACAO <> PRODUTO_VENDA
                 AND PRODUTO_VENDA <> 'OUTROS VENDA'
                 AND OPERACAO_DOTACAO_BASE <> 'QUALIDADE'
                 AND PRODUTO_VENDA_BASE <> 'ENDOSSO_GRC' THEN 1
            WHEN Lista LIKE '%NBO%' THEN 1
            WHEN Campanha LIKE '%NBO%'
                 OR Campanha IN (
                      'Residencial Venda Cross Analytics',
                      'Vida Venda Cross Analytics Ativo',
                      'Vida Venda por Oportunidade NBO - Ativo'
                    ) THEN 1
            ELSE 0
        END) AS cross_sell
    FROM (
        SELECT
            D.OPERACAO                        AS OPERACAO_DOTACAO_BASE,
            a.IDENT                           AS PRODUTO_VENDA_BASE,
            a.Campanha,
            a.Lista,
            d.rede,
            CAST(a.Dt_Inicio AS DATE)         AS dt_atendimento,
            CASE
                WHEN a.ident IN ('AUTO_VENDA','AUTO_RENOVACAO','AUTO_VENDA_FROTA','AUTO_RENOVACAO_FROTA','AUTO_ENDOSSO') THEN 'AUTO'
                WHEN a.ident = 'CREDITO_PROTEGIDO'  THEN 'CRÉDITO PROTEGIDO'
                WHEN a.ident IN ('RESIDENCIAL','EMPRESARIAL')                                                           THEN 'RESIDENCIAL'
                WHEN a.ident = 'VIDA'               THEN 'VIDA'
                WHEN a.ident = 'ITENS_PESSOAIS'     THEN 'ITENS PESSOAIS'
                WHEN a.ident = 'PATRIMONIAL'        THEN 'PATRIMONIAL'
                ELSE 'OUTROS VENDA'
            END AS PRODUTO_VENDA,
            CASE
                WHEN D.OPERACAO LIKE '%AUTO%'              THEN 'AUTO'
                WHEN D.OPERACAO LIKE '%CREDITO PROTEGIDO%' THEN 'CRÉDITO PROTEGIDO'
                WHEN D.OPERACAO LIKE '%RESIDENCIAL%'       THEN 'RESIDENCIAL'
                WHEN D.OPERACAO LIKE 'VIDA'                THEN 'VIDA'
                WHEN D.OPERACAO LIKE '%PATRIMONIAL%'       THEN 'PATRIMONIAL'
                ELSE 'OUTROS DOTACAO'
            END AS OPERACAO_DOTACAO
        FROM [gerencial].[DesempenhoHist] a
        LEFT JOIN [INFO_CENTRAL].[dotacao].[Tb_dotacao_final] AS d
            ON  d.REDE = a.Recurso_Cod
            AND CAST(a.dt_evento AS DATE) = CAST(d.DT_REFERENCIA AS DATE)
        WHERE CAST(a.dt_evento AS DATE) >= '2026-01-01'
          AND a.Classe_Processo IN (
                'AUTO - Sucesso', 'Credito Protegido - Sucesso', 'Empresarial - Sucesso',
                'Itens Pessoais - SUcesso', 'Patrimonial Comercialização - Sucesso',
                'Residencial - Sucesso', 'Vida - Sucesso'
              )
          AND a.lista NOT LIKE '%erro%'
    ) AS BASE
    GROUP BY rede, dt_atendimento
),
-- Upselling agregado por rede/data
upselling AS (
    SELECT
        RECURSO_COD                        AS rede,
        CAST(DATA_TLV AS DATE)             AS dt_atendimento,
        SUM(UP_CARRO) + SUM(UP_VIDROS) + SUM(UP_DM) + SUM(UP_DC)
            + SUM(UP_REBOQUE) + SUM(UP_FRANQUIA) AS upselling
    FROM GERENCIAL.ENDOSSOS_AUTO
    WHERE DATA_TLV >= '2026-01-01'
      AND TIPO_VALOR      = 'A PAGAR'
      AND FEZ_UPGRADE     = 'SIM'
      AND TIPO_ENDOSSO    = 'ENDOSSO GRAVADO'
    GROUP BY DATA_TLV, RECURSO_COD
),
-- Monitoria: contadores brutos por rede/data (cálculo feito na aplicação)
monitoria AS (
    SELECT
        rm.REDE,
        CAST(rm.DATA AS DATE)   AS dt_atendimento,
        SUM(N_CONFORME_ALTO)    AS mon_alto,
        SUM(N_CONFORME_MEDIO)   AS mon_medio,
        SUM(N_CONFORME_BAIXO)   AS mon_baixo,
        SUM(QTD_ELEGIVEL)       AS mon_elegivel
    FROM rv.monitoria rm (NOLOCK)
    WHERE rm.Data >= '2026-01-01'
      AND origem NOT IN ('INTERATIVA')
    GROUP BY rm.REDE, CAST(rm.DATA AS DATE)
),
-- Produto cartesiano agente × calendário com métricas reais (ou zero)
-- Inclui ROW_NUMBER para evitar duplicidade nos indicadores de rede
origem_base AS (
    SELECT
        d.colaborador, d.rede, d.supervisor, d.coordenador, d.gerente,
        d.grupo_operacao, d.operacao_sgdot, d.segmento, d.status_dotacao, d.posicao,
        c.dt                          AS dt_atendimento,
        ISNULL(r.qtd_atendimentos, 0) AS qtd_atendimentos,
        ISNULL(r.qtd_eventos,      0) AS qtd_eventos,
        ISNULL(r.qtd_cpc,          0) AS qtd_cpc,
        ISNULL(r.qtd_sucesso,      0) AS qtd_sucesso,
        ISNULL(r.total_premio,     0) AS total_premio,
        ISNULL(ret.retido,         0) AS retido,
        ISNULL(cs.cross_sell,      0) AS cross_sell,
        ISNULL(up.upselling,       0) AS upselling,
        mon.mon_alto                   AS mon_alto,
        mon.mon_medio                  AS mon_medio,
        mon.mon_baixo                  AS mon_baixo,
        mon.mon_elegivel               AS mon_elegivel,
        ROW_NUMBER() OVER (
            PARTITION BY d.rede, c.dt
            ORDER BY d.colaborador, d.operacao_sgdot
        ) AS rn_rede_dt
    FROM agent_dim d
    CROSS JOIN calendario c
    LEFT JOIN origem_real r
        ON  r.colaborador    = d.colaborador
        AND r.rede           = d.rede
        AND r.operacao_sgdot = d.operacao_sgdot
        AND r.dt_atendimento = c.dt
    LEFT JOIN retencao ret
        ON  ret.REDE           = d.rede
        AND ret.dt_atendimento = c.dt
    LEFT JOIN cross_sell cs
        ON  cs.rede           = d.rede
        AND cs.dt_atendimento = c.dt
    LEFT JOIN upselling up
        ON  up.rede           = d.rede
        AND up.dt_atendimento = c.dt
    LEFT JOIN monitoria mon
        ON  mon.REDE           = d.rede
        AND mon.dt_atendimento = c.dt
),
-- Atribui indicadores de rede apenas à primeira linha por (rede, data)
-- para evitar duplicidade ao agregar nos relatórios
origem AS (
    SELECT
        colaborador, rede, supervisor, coordenador, gerente,
        grupo_operacao, operacao_sgdot, segmento, status_dotacao, posicao,
        dt_atendimento,
        qtd_atendimentos, qtd_eventos, qtd_cpc, qtd_sucesso, total_premio,
        CASE WHEN rn_rede_dt = 1 THEN retido     ELSE 0    END AS retido,
        CASE WHEN rn_rede_dt = 1 THEN cross_sell ELSE 0    END AS cross_sell,
        CASE WHEN rn_rede_dt = 1 THEN upselling   ELSE 0    END AS upselling,
        CASE WHEN rn_rede_dt = 1 THEN mon_alto    ELSE NULL END AS mon_alto,
        CASE WHEN rn_rede_dt = 1 THEN mon_medio   ELSE NULL END AS mon_medio,
        CASE WHEN rn_rede_dt = 1 THEN mon_baixo   ELSE NULL END AS mon_baixo,
        CASE WHEN rn_rede_dt = 1 THEN mon_elegivel ELSE NULL END AS mon_elegivel,
        CASE operacao_sgdot
            WHEN 'Auto Venda Digital'      THEN 'VENDA / RENOVAÇÃO / ASSESSORIA'
            WHEN 'Auto Venda'              THEN 'VENDA / RENOVAÇÃO / ASSESSORIA'
            WHEN 'Auto Renovação Digital'  THEN 'VENDA / RENOVAÇÃO / ASSESSORIA'
            WHEN 'Auto Renovação'          THEN 'VENDA / RENOVAÇÃO / ASSESSORIA'
            WHEN 'Auto Monitorada'         THEN 'VENDA / RENOVAÇÃO / ASSESSORIA'
            WHEN 'Auto Assessoria'         THEN 'VENDA / RENOVAÇÃO / ASSESSORIA'
            WHEN 'Auto Retenção'           THEN 'RETENÇÃO'
            WHEN 'Auto Pós-Venda'          THEN 'ENDOSSO (VOZ E DIGITAL)'
            WHEN 'Auto Endosso Digital'    THEN 'ENDOSSO (VOZ E DIGITAL)'
            WHEN 'Auto Endosso Frota'      THEN 'ENDOSSO FROTA'
            WHEN 'Auto Frota'              THEN 'FROTA (VENDA E RENOVAÇÃO)'
            ELSE 'OUTROS'
        END AS tipo_operacao
    FROM origem_base
)
MERGE dbo.AgentPerformanceDaily AS destino
USING origem
    ON  ISNULL(destino.colaborador,    '') = ISNULL(origem.colaborador,    '')
    AND ISNULL(destino.rede,           '') = ISNULL(origem.rede,           '')
    AND ISNULL(destino.operacao_sgdot, '') = ISNULL(origem.operacao_sgdot, '')
    AND destino.dt_atendimento             = origem.dt_atendimento
WHEN MATCHED THEN
    UPDATE SET
        destino.supervisor       = origem.supervisor,
        destino.coordenador      = origem.coordenador,
        destino.gerente          = origem.gerente,
        destino.grupo_operacao   = origem.grupo_operacao,
        destino.segmento         = origem.segmento,
        destino.status_dotacao   = origem.status_dotacao,
        destino.posicao          = origem.posicao,
        destino.qtd_atendimentos = origem.qtd_atendimentos,
        destino.qtd_eventos      = origem.qtd_eventos,
        destino.qtd_cpc          = origem.qtd_cpc,
        destino.qtd_sucesso      = origem.qtd_sucesso,
        destino.total_premio     = origem.total_premio,
        destino.retido           = origem.retido,
        destino.cross_sell       = origem.cross_sell,
        destino.upselling        = origem.upselling,
        destino.mon_alto         = origem.mon_alto,
        destino.mon_medio        = origem.mon_medio,
        destino.mon_baixo        = origem.mon_baixo,
        destino.mon_elegivel     = origem.mon_elegivel,
        destino.tipo_operacao    = origem.tipo_operacao,
        destino.loadDate         = GETDATE()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        colaborador, rede, supervisor, coordenador, gerente,
        grupo_operacao, operacao_sgdot, segmento, status_dotacao, posicao,
        dt_atendimento, qtd_atendimentos, qtd_eventos, qtd_cpc, qtd_sucesso,
        total_premio, retido, cross_sell, upselling, mon_alto, mon_medio, mon_baixo, mon_elegivel, tipo_operacao, loadDate
    )
    VALUES (
        origem.colaborador, origem.rede, origem.supervisor, origem.coordenador, origem.gerente,
        origem.grupo_operacao, origem.operacao_sgdot, origem.segmento, origem.status_dotacao, origem.posicao,
        origem.dt_atendimento, origem.qtd_atendimentos, origem.qtd_eventos, origem.qtd_cpc, origem.qtd_sucesso,
        origem.total_premio, origem.retido, origem.cross_sell, origem.upselling, origem.mon_alto, origem.mon_medio, origem.mon_baixo, origem.mon_elegivel, origem.tipo_operacao, GETDATE()
    )
OPTION (MAXRECURSION 1000);
"""

# ============================
# 4. Execução
# ============================
start_time = time.time()

with engine.begin() as conn:
    print("Criando tabela AgentPerformanceDaily (se necessário)...")
    conn.execute(text(sql_create_table))

    print("Executando MERGE...")
    result = conn.execute(text(sql_merge))
    print(f"Linhas afetadas: {result.rowcount:,}")

elapsed = time.time() - start_time
print(f"\nFinalizado em {elapsed:.1f} segundos.")