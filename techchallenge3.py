from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import col

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)

#spark.version

tb2023_2024 = spark.read.csv("/content/drive/MyDrive/TechChallenge 3/State_of_data_BR_2023_Kaggle - df_survey_2023.csv", sep = ",", inferSchema=True, header = True)
tb2024_2025 = spark.read.csv("/content/drive/MyDrive/TechChallenge 3/Final Dataset - State of Data 2024 - Kaggle - df_survey_2024.csv", sep = ",", inferSchema=True, header = True)
tb2025_2026 = spark.read.csv("/content/drive/MyDrive/TechChallenge 3/Final Dataset - State of Data 2025-2026 - Kaggle.csv", sep = ",", inferSchema=True, header = True)

tb2023_2024 = tb2023_2024.dropDuplicates()
tb2024_2025 = tb2024_2025.dropDuplicates()
tb2025_2026 = tb2025_2026.dropDuplicates()

"""## 2023 - 2024

* Parte 1 - Dados demográficos
* Parte 2 - Dados sobre carreira
* Parte 3 - Desafios dos gestores de times de dados
* Parte 4 - Conhecimentos na área de dados
* Parte 5 - Objetivos na área de dados
* Parte 6 - Conhecimentos em Engenharia de Dados/DE
* Parte 7 - Conhecimentos em Análise de Dados/DA
* Parte 8 - Conhecimentos em Ciências de Dados/DS

"""

tb2023_2024.printSchema()

"""## 2024 - 2025

* Parte 1 - Dados demográficos
* Parte 2 - Dados sobre carreira
* Parte 3 - Desafios dos gestores de times de dados
* Parte 4 - Conhecimentos na área de dados
* Parte 5 - Objetivos na área de dados
* Parte 6 - Conhecimentos em Engenharia de Dados/DE
* Parte 7 - Conhecimentos em Análise de Dados/DA
* Parte 8 - Conhecimentos em Ciências de Dados/DS
"""

tb2024_2025.printSchema()

"""## 2025 - 2026

* Parte 1 - Dados demográficos
* Parte 2 - Dados sobre carreira
* Parte 3 - Desafios dos gestores de times de dados
* Parte 4 - Conhecimentos na área de dados
* Parte 5 - Objetivos na área de dados
* Parte 6 - Conhecimentos em Engenharia de Dados/DE
* Parte 7 - Conhecimentos em Análise de Dados/DA
* Parte 8 - Conhecimentos em Ciências de Dados/DS

Cada pergunta é dividida em Parte, Letra da Pergunta, Número da Opção escolhida

Exemplo: P3a_1 = Parte 3, pergunta (a), opção (1)

"""

tb2025_2026.printSchema()

print(f"2023:{tb2023_2024.count()}\n2024:{tb2024_2025.count()}\n2025:{tb2025_2026.count()}")

"""## Perguntas"""

def findCols(df, prefix):
    return [c for c in df.columns if c.startswith(prefix)]

"""### Como está estruturado o mercado brasileiro de dados?

Referencia da web:

* O mercado de dados no Brasil vive forte expansão em análise e infraestrutura impulsionada por inteligência artificial
* Evolução no nível técnico
* Os salários subiram em média 6,7%, superando os índices inflacionários do período.
"""

def process_year(df, mapping, year):
    exprs = [F.trim(F.col(f"`{old}`")).alias(new) for old, new in mapping.items()]
    return df.select(*exprs) \
             .groupBy(*join_cols) \
             .agg(F.count("*").alias(f"Quantidade_{year}"))

join_cols = [
    "Faixa_Salarial", "Situacao", "Setor", "Tempo De Experiencia Dados",
    "Tempo De Experiencia TI", "Satisfação", "Flexibilidade de Trabalho Remoto",
    "Oportunidade de Aprendizado", "Plano de Carreira", "Mudança de Area"
]

map_2023 = {
    "('P2_h ', 'Faixa salarial')": "Faixa_Salarial",
    "('P2_a ', 'Qual sua situação atual de trabalho?')": "Situacao",
    "('P2_b ', 'Setor')": "Setor",
    "('P2_i ', 'Quanto tempo de experiência na área de dados você tem?')": "Tempo De Experiencia Dados",
    "('P2_j ', 'Quanto tempo de experiência na área de TI/Engenharia de Software você teve antes de começar a trabalhar na área de dados?')": "Tempo De Experiencia TI",
    "('P2_k ', 'Você está satisfeito na sua empresa atual?')": "Satisfação",
    "('P2_o_4 ', 'Flexibilidade de trabalho remoto')": "Flexibilidade de Trabalho Remoto",
    "('P2_o_6 ', 'Oportunidade de aprendizado e trabalhar com referências na área')": "Oportunidade de Aprendizado",
    "('P2_o_7 ', 'Plano de carreira e oportunidades de crescimento profissional')": "Plano de Carreira",
    "('P2_l_4 ', 'Gostaria de trabalhar em em outra área de atuação')": "Mudança de Area"
}

map_modern = {
    "2.h_faixa_salarial": "Faixa_Salarial",
    "2.a_situação_de_trabalho": "Situacao",
    "2.b_setor": "Setor",
    "2.i_tempo_de_experiencia_em_dados": "Tempo De Experiencia Dados",
    "2.j_tempo_de_experiencia_em_ti": "Tempo De Experiencia TI",
    "2.k_satisfeito_atualmente": "Satisfação",
    "2.l.4_Flexibilidade de trabalho remoto": "Flexibilidade de Trabalho Remoto",
    "2.l.6_Oportunidade de aprendizado e trabalhar com referências": "Oportunidade de Aprendizado",
    "2.l.7_Oportunidades de crescimento": "Plano de Carreira",
    "2.l.11_Gostaria de trabalhar em outra área": "Mudança de Area"
}

stats_2023 = process_year(tb2023_2024, map_2023, "2023")
stats_2024 = process_year(tb2024_2025, map_modern, "2024")
stats_2025 = process_year(tb2025_2026, map_modern, "2025")


combined_stats = stats_2023.join(stats_2024, on=join_cols, how="outer") \
                           .join(stats_2025, on=join_cols, how="outer") \
                           .fillna(0) \
                           .orderBy("Faixa_Salarial", "Situacao")

combined_stats.show(n=20, truncate=False)

unpivoted_stats = combined_stats.selectExpr(
    "`Faixa_Salarial`",
    "`Situacao`",
    "`Setor`",
    "`Tempo De Experiencia Dados`",
    "`Tempo De Experiencia TI`",
    "`Satisfação`",
    "`Flexibilidade de Trabalho Remoto`",
    "`Oportunidade de Aprendizado`",
    "`Plano de Carreira`",
    "`Mudança de Area`",
    "stack(3, '2023', Quantidade_2023, '2024', Quantidade_2024, '2025', Quantidade_2025) as (Ano, Quantidade)"
)

unpivoted_stats = unpivoted_stats.withColumn("Satisfação", col("Satisfação").try_cast("int"))
unpivoted_stats = unpivoted_stats.withColumn("Flexibilidade de Trabalho Remoto", col("Flexibilidade de Trabalho Remoto").try_cast("int"))
unpivoted_stats = unpivoted_stats.withColumn("Oportunidade de Aprendizado", col("Oportunidade de Aprendizado").try_cast("int"))
unpivoted_stats = unpivoted_stats.withColumn("Plano de Carreira", col("Plano de Carreira").try_cast("int"))
unpivoted_stats = unpivoted_stats.withColumn("Mudança de Area", col("Mudança de Area").try_cast("int"))


unpivoted_stats.orderBy("Faixa_Salarial", "Situacao", "Ano")\
          .fillna(0)\
          .show(n=20, truncate=False)

"""unpivoted_stats.orderBy("Faixa_Salarial", "Situacao", "Ano")\
          .fillna(0)\
          .write.csv("/content/drive/MyDrive/TechChallenge 3/Estrutura_Mercado_V2.csv", header=True, mode="overwrite")"""
#unpivoted_stats.write.csv("/content/drive/MyDrive/TechChallenge 3/Estrutura_Mercado.csv", header=True, mode="overwrite")

"""### Qual é o cenário de diversidade de gênero nas carreiras de dados?"""

print([col for col in tb2024_2025.columns if col.startswith("1.e")])

join_cols = [
    "Idade", "Genero", "Cor/Raça/Etnia", "PCD",
    "Não Prejudicado","Prejudicado_Etnia", "Prejudicado_Genero", "Prejudicado_PCD",
    "Quantidade de oportunidades", "Aprovação_em_processos",
    "Nível_de_cobrança", "Relação_com_outras_pessoas", "Aspetos_prejudicados",
    "UF_de_origem", "Nivel_de_ensino", "Área_de_formação"
]

map_2023 = {"('P1_a_1 ', 'Faixa idade')":"Idade",
          "('P1_b ', 'Genero')":"Genero",
          "('P1_c ', 'Cor/raca/etnia')":"Cor/Raça/Etnia",
          "('P1_d ', 'PCD')":"PCD",
          "('P1_e_1 ', 'Não acredito que minha experiência profissional seja afetada')":"Não Prejudicado",
          "('P1_e_2 ', 'Experiencia prejudicada devido a minha Cor Raça Etnia')":"Prejudicado_Etnia",
          "('P1_e_3 ', 'Experiencia prejudicada devido a minha identidade de gênero')":"Prejudicado_Genero",
          "('P1_e_4 ', 'Experiencia prejudicada devido ao fato de ser PCD')":"Prejudicado_PCD",
          "('P1_f_6', 'Nível de cobrança no trabalho/Stress no trabalho')":"Quantidade de oportunidades",
          "('P1_f ', 'aspectos_prejudicados')":"Aprovação_em_processos",
          "('P1_f_1', 'Quantidade de oportunidades de emprego/vagas recebidas')":"Nível_de_cobrança",
          "('P1_f_3', 'Aprovação em processos seletivos/entrevistas')":"Relação_com_outras_pessoas",
          "('P1_f_8', 'Relação com outros membros da empresa, em momentos de trabalho')":"Aspetos_prejudicados",
          "('P1_k ', 'Regiao de origem')":"UF_de_origem",
          "('P1_l ', 'Nivel de Ensino')":"Nivel_de_ensino",
          "('P1_m ', 'Área de Formação')":"Área_de_formação"
}

map_modern = {"1.a.1_faixa_idade":"Idade",
              "1.b_genero":"Genero",
              "1.c_cor/raca/etnia":"Cor/Raça/Etnia",
              "1.d_pcd":"PCD",
              '1.e.1_Não acredito que minha experiência profissional seja afetada':"Não Prejudicado",
              '1.e.2_Sim, devido a minha Cor/Raça/Etnia':"Prejudicado_Etnia",
              '1.e.3_Sim, devido a minha identidade de gênero':"Prejudicado_Genero",
              '1.e.4_Sim, devido ao fato de ser PCD':"Prejudicado_PCD",
              "1.f.1_Quantidade de oportunidades de emprego/vagas recebidas":"Quantidade de oportunidades",
              "1.f.3_Aprovação em processos seletivos/entrevistas":"Aprovação_em_processos",
              "1.f.6_Nível de cobrança no trabalho/Stress no trabalho":"Nível_de_cobrança",
              "1.f.8_Relação com outras pessoas da empresa, em momentos de trabalho": "Relação_com_outras_pessoas",
              "1.f_aspectos_prejudicados":"Aspetos_prejudicados",
              "1.k.1_uf_de_origem":"UF_de_origem",
              "1.l_nivel_de_ensino":"Nivel_de_ensino",
              "1.m_área_de_formação":"Área_de_formação"
}

stats_2023 = process_year(tb2023_2024, map_2023, "2023")
stats_2024 = process_year(tb2024_2025, map_modern, "2024")
stats_2025 = process_year(tb2025_2026, map_modern, "2025")

combined_stats = stats_2023.join(stats_2024, on=join_cols, how="outer") \
                           .join(stats_2025, on=join_cols, how="outer") \
                           .fillna(0) \
                           .orderBy("Idade", "Genero", "Cor/Raça/Etnia", "PCD")

combined_stats.show(n=20, truncate=False)

unpivoted_stats = combined_stats.selectExpr(
    "`Idade`",
    "`Genero`",
    "`Cor/Raça/Etnia`",
    "`PCD`",
    "`Não Prejudicado`",
    "`Prejudicado_Etnia`",
    "`Prejudicado_Genero`",
    "`Prejudicado_PCD`",
    "`Quantidade de oportunidades`",
    "`Aprovação_em_processos`",
    "`Nível_de_cobrança`",
    "`Relação_com_outras_pessoas`",
    "`Aspetos_prejudicados`",
    "`UF_de_origem`",
    "`Nivel_de_ensino`",
    "`Área_de_formação`",
    "stack(3, '2023', Quantidade_2023, '2024', Quantidade_2024, '2025', Quantidade_2025) as (Ano, Quantidade)"
)

unpivoted_stats = unpivoted_stats.withColumn("Quantidade", col("Quantidade").try_cast("int"))
unpivoted_stats = unpivoted_stats.withColumn("Quantidade de oportunidades", col("Quantidade").try_cast("int"))
unpivoted_stats = unpivoted_stats.withColumn("Não Prejudicado", col("Não Prejudicado").try_cast("int"))
unpivoted_stats = unpivoted_stats.withColumn("Prejudicado_Etnia", col("Prejudicado_Etnia").try_cast("int"))
unpivoted_stats = unpivoted_stats.withColumn("Prejudicado_Genero", col("Prejudicado_Genero").try_cast("int"))
unpivoted_stats = unpivoted_stats.withColumn("Prejudicado_PCD", col("Prejudicado_PCD").try_cast("int"))

unpivoted_stats.orderBy("Idade", "Genero", "Cor/Raça/Etnia", "PCD")\
                .fillna(0)\
                .show(n=20, truncate=False)

unpivoted_stats.orderBy("Idade", "Genero", "Cor/Raça/Etnia", "PCD")\
                .fillna(0)\
                .write.csv("/content/drive/MyDrive/TechChallenge 3/Diversidade_Genero_V2.csv", header=True, mode="overwrite")
#unpivoted_stats.write.csv("/content/drive/MyDrive/TechChallenge 3/Diversidade_Genero.csv", header=True, mode="overwrite")
#unpivoted_stats.show(20, truncate=False)

"""### Quais tecnologias apresentam maior adoção entre os profissionais?

"""

mapping = []

bi_2023 = ["Microsoft PowerBI", "Qlik View/Qlik Sense", "Tableau", "Metabase", "Superset", "Redash", "Looker",
    "Looker Studio(Google Data Studio)", "Amazon Quicksight", "Mode", "Alteryx", "MicroStrategy",
    "IBM Analytics/Cognos", "SAP Business Objects/SAP Analytics", "Oracle Business Intelligence",
    "Salesforce/Einstein Analytics", "Birst", "SAS Visual Analytics", "Grafana", "TIBCO Spotfire", "Pentaho",
    "Fazemos todas as análises utilizando apenas Excel ou planilhas do google",
    "Não utilizo nenhuma ferramenta de BI no trabalho"]
bi_dotyears = ["Microsoft PowerBI", "Qlik View/Qlik Sense", "Tableau", "Metabase", "Superset", "Redash", "Looker",
    "Looker Studio(Google Data Studio)", "Amazon Quicksight", "Alteryx", "SAP Business Objects/SAP Analytics",
    "Oracle Business Intelligence", "Salesforce/Einstein Analytics", "SAS Visual Analytics", "Grafana", "Pentaho",
    "Fazemos todas as análises utilizando apenas Excel ou planilhas do google",
    "Não utilizo nenhuma ferramenta de BI no trabalho"]

bi_all = sorted(set(bi_2023) | set(bi_dotyears))
for tech in bi_all:
    c23 = f"('P4_j_{bi_2023.index(tech) + 1} ', '{tech}')" if tech in bi_2023 else None
    c24 = f"4.j.{bi_dotyears.index(tech) + 1}_{tech}" if tech in bi_dotyears else None
    c25 = f"4.g.{bi_dotyears.index(tech) + 1}_{tech}" if tech in bi_dotyears else None
    mapping.append(("Ferramentas de BI", tech, c23, c24, c25))


db_names = ["MySQL", "Oracle", "SQL SERVER", "Amazon Aurora ou RDS", "DynamoDB", "CoachDB", "Cassandra",
    "MongoDB", "MariaDB", "Datomic", "S3", "PostgreSQL", "ElasticSearch", "DB2", "Microsoft Access", "SQLite",
    "Sybase", "Firebase", "Vertica", "Redis", "Neo4J", "Google BigQuery", "Google Firestore", "Amazon Redshift",
    "Amazon Athena", "Snowflake", "Databricks", "HBase", "Presto", "Splunk", "SAP HANA", "Hive", "Firebird"]
for i, tech in enumerate(db_names, start=1):
    mapping.append((
        "Bancos de Dados", tech,
        f"('P4_g_{i} ', '{tech}')",
        f"4.g.{i}_{tech}",
        f"4.d.{i}_{tech}",
    ))

cloud = [
    ("Azure (Microsoft)", "('P4_h_1 ', 'Azure (Microsoft)')", "4.h.3_Azure (Microsoft)", "4.e.3_Azure (Microsoft)"),
    ("Amazon Web Services (AWS)", "('P4_h_2 ', 'Amazon Web Services (AWS)')", "4.h.1_Amazon Web Services (AWS)", "4.e.1_Amazon Web Services (AWS)"),
    ("Google Cloud (GCP)", "('P4_h_3 ', 'Google Cloud (GCP)')", "4.h.2_Google Cloud (GCP)", "4.e.2_Google Cloud (GCP)"),
    ("Oracle Cloud", "('P4_h_4 ', 'Oracle Cloud')", "4.h.4_Oracle Cloud", "4.e.4_Oracle Cloud"),
    ("IBM", "('P4_h_5 ', 'IBM')", "4.h.5_IBM", "4.e.5_IBM"),
    ("Servidores On Premise/Não utilizamos Cloud", "('P4_h_6 ', 'Servidores On Premise/Não utilizamos Cloud')", "4.h.6_Servidores On Premise/Não utilizamos Cloud", "4.e.6_Servidores On Premise/Não utilizamos Cloud"),
    ("Cloud Própria", "('P4_h_7 ', 'Cloud Própria')", "4.h.7_Cloud Própria", "4.e.7_Cloud Própria"),
]
for tech, c23, c24, c25 in cloud:
    mapping.append(("Cloud", tech, c23, c24, c25))

linguagens = [
    ("SQL", "('P4_d_1 ', 'SQL')", "4.d.1_SQL", "4.c.1_SQL"),
    ("R", "('P4_d_2 ', 'R ')", "4.d.2_R", "4.c.2_R"),
    ("Python", "('P4_d_3 ', 'Python')", "4.d.3_Python", "4.c.3_Python"),
    ("C/C++/C#", "('P4_d_4 ', 'C/C++/C#')", "4.d.4_C/C++/C#", "4.c.4_C/C++/C#"),
    (".NET", "('P4_d_5 ', '.NET')", "4.d.5_.NET", None),
    ("Java", "('P4_d_6 ', 'Java')", "4.d.6_Java", None),
    ("Julia", "('P4_d_7 ', 'Julia')", "4.d.7_Julia", "4.c.5_Julia"),
    ("SAS/Stata", "('P4_d_8 ', 'SAS/Stata')", "4.d.8_SAS/Stata", None),
    ("Visual Basic/VBA", "('P4_d_9 ', 'Visual Basic/VBA')", "4.d.9_Visual Basic/VBA", "4.c.6_Visual Basic/VBA"),
    ("Scala", "('P4_d_10 ', 'Scala')", "4.d.10_Scala", "4.c.7_Scala"),
    ("Matlab", "('P4_d_11 ', 'Matlab')", "4.d.11_Matlab", None),
    ("Rust", "('P4_d_12 ', 'Rust')", "4.d.12_Rust", "4.c.9_Rust"),
    ("PHP", "('P4_d_13 ', 'PHP')", "4.d.13_PHP", None),
    ("JavaScript", "('P4_d_14 ', 'JavaScript')", "4.d.14_JavaScript", None),
    ("DAX", None, None, "4.c.8_DAX"),
]
for tech, c23, c24, c25 in linguagens:
    mapping.append(("Linguagens", tech, c23, c24, c25))

def safe_col(name):
    """Referencia colunas com caracteres especiais (parênteses, vírgulas, pontos)."""
    return F.col("`" + name.replace("`", "``") + "`").cast("string")

def build_year_result(df, year_label, col_index):
    """Conta quantos respondentes marcaram cada tecnologia (valor não nulo e != '0')."""
    total = df.count()
    rows = [(cat, tech, [c23, c24, c25][col_index])
            for cat, tech, c23, c24, c25 in mapping
            if [c23, c24, c25][col_index] is not None]

    exprs = [
        F.sum(
            F.when(
                safe_col(c).isNotNull() & (F.trim(safe_col(c)) != "0") & (F.trim(safe_col(c)) != ""),
                1
            ).otherwise(0)
        ).alias(f"idx{i}")
        for i, (cat, tech, c) in enumerate(rows)
    ]
    agg = df.agg(*exprs).collect()[0]

    result = []
    for i, (cat, tech, c) in enumerate(rows):
        cnt = agg[f"idx{i}"]
        pct = round(100.0 * cnt / total, 2) if total else 0.0
        result.append((year_label, cat, tech, int(cnt), int(total), pct))
    return result

resultado = []
resultado += build_year_result(tb2023_2024, "2023", 0)
resultado += build_year_result(tb2024_2025, "2024", 1)
resultado += build_year_result(tb2025_2026, "2025-2026", 2)

df_tecnologias = spark.createDataFrame(
    resultado,
    ["ano_pesquisa", "categoria", "tecnologia", "qtd_respondentes", "total_respondentes", "percentual_adocao"]
)

df_tecnologias = df_tecnologias.orderBy(F.col("ano_pesquisa"), F.col("categoria"), F.desc("percentual_adocao"))
df_tecnologias.show(50, truncate=False)

df_tecnologias.write.csv("/content/drive/MyDrive/TechChallenge 3/Tecnologia.csv", header=True, mode="overwrite")

"""### Impactos da IA"""

def renomear_seguro(df, prefixo):
    originais = df.columns
    seguros = [f"{prefixo}_{i}" for i in range(len(originais))]
    return df.toDF(*seguros), dict(zip(originais, seguros))

tb2023_s, map2023 = renomear_seguro(tb2023_2024, "c23")
tb2024_s, map2024 = renomear_seguro(tb2024_2025, "c24")
tb2025_s, map2025 = renomear_seguro(tb2025_2026, "c25")

anos_df = {"2023": tb2023_s, "2024": tb2024_s, "2025-2026": tb2025_s}
anos_map = {"2023": map2023, "2024": map2024, "2025-2026": map2025}
anos_total = {"2023": tb2023_s.count(), "2024": tb2024_s.count(), "2025-2026": tb2025_s.count()}

prioridade_col = {
    "2023": "('P3_e ', 'AI Generativa é uma prioridade em sua empresa?')",
    "2024": "3.e_ai_generativa_e_llm_é_uma_prioridade?",
    "2025-2026": "3.e_ai_generativa_e_llm_é_uma_prioridade?",
}


tipos_uso_empresa = {
    "2023": [
        ("Uso descentralizado pelos colaboradores", "('P3_f_1 ', 'Colaboradores usando AI generativa de forma independente e descentralizada')"),
        ("Direcionamento centralizado", "('P3_f_2 ', 'Direcionamento centralizado do uso de AI generativa')"),
        ("Devs usando Copilots", "('P3_f_3 ', 'Desenvolvedores utilizando Copilots')"),
        ("Melhoria de produtos externos", "('P3_f_4 ', 'AI Generativa e LLMs para melhorar produtos externos')"),
        ("Melhoria de produtos internos", "('P3_f_5 ', 'AI Generativa e LLMs para melhorar produtos internos para os colaboradores')"),
        ("Principal frente do negócio", "('P3_f_6 ', 'IA Generativa e LLMs como principal frente do negócio')"),
        ("Não é prioridade", "('P3_f_7 ', 'IA Generativa e LLMs não é prioridade')"),
        ("Não sabe opinar", "('P3_f_8 ', 'Não sei opinar sobre o uso de IA Generativa e LLMs na empresa')"),
    ],
    "2024": [
        ("Uso descentralizado pelos colaboradores", "3.f.1 Colaboradores usando AI generativa de forma independente e descentralizada"),
        ("Direcionamento centralizado", "3.f.2 Direcionamento centralizado do uso de AI generativa"),
        ("Devs usando Copilots", "3.f.3 Desenvolvedores utilizando Copilots"),
        ("Melhoria de produtos externos", "3.f.4 AI Generativa e LLMs para melhorar produtos externos para os clientes finais"),
        ("Melhoria de produtos internos", "3.f.5 AI Generativa e LLMs para melhorar produtos internos para os colaboradores"),
        ("Principal frente do negócio", "3.f.6 IA Generativa e LLMs como principal frente do negócio"),
        ("Não é prioridade", "3.f.7 IA Generativa e LLMs não é prioridade"),
        ("Não sabe opinar", "3.f.8 Não sei opinar sobre o uso de IA Generativa e LLMs na empresa"),
    ],
    "2025-2026": [
        ("Uso descentralizado pelos colaboradores", "3.f.1 Colaboradores usando AI generativa de forma independente e descentralizada"),
        ("Direcionamento centralizado", "3.f.2 Direcionamento centralizado do uso de AI generativa"),
        ("Devs usando Copilots", "3.f.3 Desenvolvedores utilizando Copilots"),
        ("Melhoria de produtos externos", "3.f.4 AI Generativa e LLMs para melhorar produtos externos para os clientes finais"),
        ("Melhoria de produtos internos", "3.f.5 AI Generativa e LLMs para melhorar produtos internos para os colaboradores"),
        ("Principal frente do negócio", "3.f.6 IA Generativa e LLMs como principal frente do negócio"),
        ("Não é prioridade", "3.f.7 IA Generativa e LLMs não é prioridade"),
        ("Não sabe opinar", "3.f.8 Não sei opinar sobre o uso de IA Generativa e LLMs na empresa"),
    ],
}


impacto_resultados_col = {
    "2025-2026": "3.g_empresa_está_conseguindo_ter_bons_resultados_com_llms",
}


motivos_nao_usar = {
    "2023": [
        ("Falta de compreensão dos casos de uso", "('P3_g_1 ', 'Falta de compreensão dos casos de uso')"),
        ("Falta de confiabilidade (alucinação)", "('P3_g_2 ', 'Falta de confiabilidade das saídas (alucinação dos modelos)')"),
        ("Incerteza regulatória", "('P3_g_3 ', 'Incerteza em relação a regulamentação')"),
        ("Segurança/privacidade de dados", "('P3_g_4 ', 'Preocupações com segurança e privacidade de dados')"),
        ("ROI não comprovado", "('P3_g_5 ', 'Retorno sobre investimento (ROI) não comprovado de IA Generativa')"),
        ("Dados da empresa não prontos", "('P3_g_6 ', 'Dados da empresa não estão prontos para uso de IA Generativa')"),
        ("Falta de expertise/recursos", "('P3_g_7 ', 'Falta de expertise ou falta de recursos')"),
        ("Alta direção não vê valor", "('P3_g_8 ', 'Alta direção da empresa não vê valor ou não vê como prioridade')"),
        ("Preocupações com propriedade intelectual", "('P3_g_9 ', 'Preocupações com propriedade intelectual')"),
    ],
    "2024": [
        ("Falta de compreensão dos casos de uso", "3.g.1 Falta de compreensão dos casos de uso"),
        ("Falta de confiabilidade (alucinação)", "3.g.2 Falta de confiabilidade das saídas (alucinação dos modelos)"),
        ("Incerteza regulatória", "3.g.3 Incerteza em relação a regulamentação"),
        ("Segurança/privacidade de dados", "3.g.4 Preocupações com segurança e privacidade de dados"),
        ("ROI não comprovado", "3.g.5 Retorno sobre investimento (ROI) não comprovado de IA Generativa"),
        ("Dados da empresa não prontos", "3.g.6 Dados da empresa não estão prontos para uso de IA Generativa"),
        ("Falta de expertise/recursos", "3.g.7 Falta de expertise ou falta de recursos"),
        ("Alta direção não vê valor", "3.g.8 Alta direção da empresa não vê valor ou não vê como prioridade"),
        ("Preocupações com propriedade intelectual", "3.g.9 Preocupações com propriedade intelectual"),
    ],
    "2025-2026": [
        ("Falta de compreensão dos casos de uso", "3.h.1 Falta de compreensão dos casos de uso"),
        ("Falta de confiabilidade (alucinação)", "3.h.2 Falta de confiabilidade das saídas (alucinação dos modelos)"),
        ("Incerteza regulatória", "3.h.3 Incerteza em relação a regulamentação"),
        ("Segurança/privacidade de dados", "3.h.4 Preocupações com segurança e privacidade de dados"),
        ("ROI não comprovado", "3.h.5 Retorno sobre investimento (ROI) não comprovado de IA Generativa"),
        ("Dados da empresa não prontos", "3.h.6 Dados da empresa não estão prontos para uso de IA Generativa"),
        ("Falta de expertise/recursos", "3.h.7 Falta de expertise ou falta de recursos"),
        ("Alta direção não vê valor", "3.h.8 Alta direção da empresa não vê valor ou não vê como prioridade"),
        ("Preocupações com propriedade intelectual", "3.h.9 Preocupações com propriedade intelectual"),
    ],
}

# --- 3e) Uso individual de IA Generativa com foco em produtividade (ChatGPT/Copilot) ---
uso_individual = {
    "2023": [
        ("Não uso", "('P4_m_1 ', 'Não uso soluções de AI Generativa com foco em produtividade')"),
        ("Uso gratuito", "('P4_m_2 ', 'Uso soluções gratuitas de AI Generativa com foco em produtividade')"),
        ("Uso e pago (pessoal)", "('P4_m_3 ', 'Uso e pago pelas soluções de AI Generativa com foco em produtividade')"),
        ("Empresa paga", "('P4_m_4 ', 'A empresa que trabalho paga pelas soluções de AI Generativa com foco em produtividade')"),
        ("Uso tipo Copilot", "('P4_m_5 ', 'Uso soluções do tipo Copilot')"),
    ],
    "2024": [
        ("Não uso", "4.m.1 Não uso soluções de AI Generativa com foco em produtividade"),
        ("Uso gratuito", "4.m.2 Uso soluções gratuitas de AI Generativa com foco em produtividade"),
        ("Uso e pago (pessoal)", "4.m.3 Uso e pago pelas soluções de AI Generativa com foco em produtividade"),
        ("Empresa paga", "4.m.4 A empresa que trabalho paga pelas soluções de AI Generativa com foco em produtividade"),
        ("Uso tipo Copilot", "4.m.5 Uso soluções do tipo Copilot"),
    ],
    "2025-2026": [
        ("Não uso", "4.j.1 Não uso soluções de AI Generativa com foco em produtividade"),
        ("Uso gratuito", "4.j.2 Uso soluções gratuitas de AI Generativa com foco em produtividade"),
        ("Uso e pago (pessoal)", "4.j.3 Uso e pago pelas soluções de AI Generativa com foco em produtividade"),
        ("Empresa paga", "4.j.4 A empresa que trabalho paga pelas soluções de AI Generativa com foco em produtividade"),
        ("Uso tipo Copilot", "4.j.5 Uso soluções do tipo Copilot"),
    ],
}

def calcular_flags(df_seguro, mapa_original_para_seguro, mapeamento, dimensao, ano, total):
    linhas = []
    for label, coluna_original in mapeamento:
        coluna_segura = mapa_original_para_seguro.get(coluna_original)
        if coluna_segura is not None:
            # Corrected: Compare with string "1" instead of integer 1
            qtd = df_seguro.filter(F.col(coluna_segura) == "1").count()
            pct = round((qtd / total) * 100, 2) if total > 0 else 0
            linhas.append((ano, dimensao, label, qtd, total, pct))
    return linhas

def calcular_categorica(df_seguro, mapa_original_para_seguro, coluna_original, dimensao, ano, total):
    linhas = []
    coluna_segura = mapa_original_para_seguro.get(coluna_original)
    if coluna_segura is None:
        return linhas
    dist = df_seguro.filter(F.col(coluna_segura).isNotNull()).groupBy(F.col(coluna_segura)).count().collect()
    for row in dist:
        valor = row[coluna_segura]
        qtd = row["count"]
        pct = round((qtd / total) * 100, 2) if total > 0 else 0
        linhas.append((ano, dimensao, valor, qtd, total, pct))
    return linhas

resultados = []

for ano in ["2023", "2024", "2025-2026"]:
    df, mapa, total = anos_df[ano], anos_map[ano], anos_total[ano]

    # 5a) Prioridade de IA na empresa (categórica)
    resultados += calcular_categorica(df, mapa, prioridade_col[ano], "Prioridade de IA na Empresa", ano, total)

    # 5b) Tipos de uso na empresa
    resultados += calcular_flags(df, mapa, tipos_uso_empresa[ano], "Tipo de Uso na Empresa", ano, total)

    # 5c) Impacto: empresa consegue bons resultados com LLMs (só 2025-2026)
    if ano in impacto_resultados_col:
        resultados += calcular_categorica(df, mapa, impacto_resultados_col[ano], "Impacto - Bons Resultados com LLMs", ano, total)

    # 5d) Motivos para não usar (barreiras ao impacto)
    resultados += calcular_flags(df, mapa, motivos_nao_usar[ano], "Motivo para Não Usar IA", ano, total)

    # 5e) Uso individual para produtividade
    resultados += calcular_flags(df, mapa, uso_individual[ano], "Uso Individual (Produtividade)", ano, total)

df_resultado = spark.createDataFrame(
    resultados,
    ["ano", "dimensao", "categoria_resposta", "qtd_respondentes", "total_respondentes_ano", "pct"]
).orderBy(F.col("ano"), F.col("dimensao"), F.col("pct").desc())

df_resultado.show(50, truncate=False)

df_resultado.write.csv("/content/drive/MyDrive/TechChallenge 3/indice_adocao_impacto_ia.csv", header=True, mode="overwrite")

"""### Diferencas de regiao"""

col_salario = {
    "2023": "('P2_h ', 'Faixa salarial')",
    "2024": "2.h_faixa_salarial",
    "2025-2026": "2.h_faixa_salarial",
}
col_nivel = {
    "2023": "('P2_g ', 'Nivel')",
    "2024": "2.g_nivel",
    "2025-2026": "2.g_nivel",
}
col_regiao = {
    "2023": "('P1_i_2 ', 'Regiao onde mora')",
    "2024": "1.i.2_regiao_onde_mora",
    "2025-2026": "1.i.2_regiao_onde_mora",
}
col_modelo_trabalho = {
    "2023": "('P2_r ', 'Atualmente qual a sua forma de trabalho?')",
    "2024": "2.r_modelo_de_trabalho_atual",     # nome da pergunta muda de 2.r (2024) para 2.q (2025-2026)
    "2025-2026": "2.q_modelo_de_trabalho_atual",
}

faixa_para_ponto_medio = {
    "de R$ 1.001/mês a R$ 2.000/mês": 1500,
    "de R$ 2.001/mês a R$ 3.000/mês": 2500,
    "de R$ 3.001/mês a R$ 4.000/mês": 3500,
    "de R$ 4.001/mês a R$ 6.000/mês": 5000,
    "de R$ 6.001/mês a R$ 8.000/mês": 7000,
    "de R$ 8.001/mês a R$ 12.000/mês": 10000,
    "de R$ 12.001/mês a R$ 16.000/mês": 14000,
    "de R$ 16.001/mês a R$ 20.000/mês": 18000,
    "de R$ 20.001/mês a R$ 25.000/mês": 22500,
    "de R$ 25.001/mês a R$ 30.000/mês": 27500,
    "de R$ 30.001/mês a R$ 40.000/mês": 35000,
    "Acima de R$ 40.001/mês": 45000,
}
mapping_expr = F.create_map([F.lit(x) for pair in faixa_para_ponto_medio.items() for x in pair])

def salario_por_dimensao(df_seguro, mapa_original_para_seguro, coluna_salario_original,
                          coluna_dimensao_original, nome_dimensao, ano):
    c_sal = mapa_original_para_seguro.get(coluna_salario_original)
    c_dim = mapa_original_para_seguro.get(coluna_dimensao_original)
    if c_sal is None or c_dim is None:
        return []

    df_calc = (
        df_seguro
        .filter(F.col(c_sal).isNotNull() & F.col(c_dim).isNotNull())
        .withColumn("ponto_medio", mapping_expr[F.col(c_sal)])
        .filter(F.col("ponto_medio").isNotNull())
    )

    agregado = (
        df_calc.groupBy(F.col(c_dim).alias("categoria"))
        .agg(
            F.round(F.avg("ponto_medio"), 0).alias("salario_medio_estimado"),
            F.count("*").alias("qtd_respondentes")
        )
        .collect()
    )
    return [(ano, nome_dimensao, row["categoria"], int(row["salario_medio_estimado"]), row["qtd_respondentes"]) for row in agregado]


resultados = []
for ano in ["2023", "2024", "2025-2026"]:
    df, mapa = anos_df[ano], anos_map[ano]
    resultados += salario_por_dimensao(df, mapa, col_salario[ano], col_regiao[ano], "Regiao", ano)
    resultados += salario_por_dimensao(df, mapa, col_salario[ano], col_nivel[ano], "Senioridade", ano)
    resultados += salario_por_dimensao(df, mapa, col_salario[ano], col_modelo_trabalho[ano], "Modelo de Trabalho", ano)

df_resultado = spark.createDataFrame(
    resultados,
    ["ano", "dimensao", "categoria", "salario_medio_estimado", "qtd_respondentes"]
).orderBy(F.col("ano"), F.col("dimensao"), F.col("salario_medio_estimado").desc())

df_resultado.show(50, truncate=False)

df_resultado.write.csv("/content/drive/MyDrive/TechChallenge 3/Diferencas_Regiao.csv", header=True, mode="overwrite")

"""### Diferenças Região V2"""

join_cols = [
    "Idade", "Setor","Situação de Trabalho", "Faixa Salarial",
    "Nivel", "Regiao onde mora", "Atualmente qual a sua forma de trabalho?"
]

map_2023 = {
    "('P1_a_1 ', 'Faixa idade')":"Idade",
    "('P2_b ', 'Setor')":"Setor",
    "('P2_a ', 'Qual sua situação atual de trabalho?')":"Situação de Trabalho",
    "('P2_h ', 'Faixa salarial')":"Faixa Salarial",
    "('P2_g ', 'Nivel')":"Nivel",
    "('P1_i_2 ', 'Regiao onde mora')":"Regiao onde mora",
    "('P2_r ', 'Atualmente qual a sua forma de trabalho?')":"Atualmente qual a sua forma de trabalho?",
}

map_2024={
    '1.a.1_faixa_idade':"Idade",
    '2.b_setor':"Setor",
    '2.a_situação_de_trabalho':"Situação de Trabalho",
    "2.h_faixa_salarial":"Faixa Salarial",
    "2.g_nivel":"Nivel",
    "1.i.2_regiao_onde_mora":"Regiao onde mora",
    "2.r_modelo_de_trabalho_atual":"Atualmente qual a sua forma de trabalho?"
}

map_2025={
    '1.a.1_faixa_idade':"Idade",
    '2.b_setor':"Setor",
    '2.a_situação_de_trabalho':"Situação de Trabalho",
    "2.h_faixa_salarial":"Faixa Salarial",
    "2.g_nivel":"Nivel",
    "1.i.2_regiao_onde_mora":"Regiao onde mora",
    "2.q_modelo_de_trabalho_atual":"Atualmente qual a sua forma de trabalho?"
}
stats_2023 = process_year(tb2023_2024, map_2023, "2023")
stats_2024 = process_year(tb2024_2025, map_2024, "2024")
stats_2025 = process_year(tb2025_2026, map_2025, "2025")

combined_stats = stats_2023.join(stats_2024, on=join_cols, how="outer") \
                           .join(stats_2025, on=join_cols, how="outer") \
                           .fillna(0)

unpivoted_stats = combined_stats.selectExpr(
    "`Idade`",
    "`Setor`",
    "`Situação de Trabalho`",
    "`Faixa Salarial`",
    "`Nivel`",
    "`Regiao onde mora`",
    "`Atualmente qual a sua forma de trabalho?`",
    "stack(3, '2023', Quantidade_2023, '2024', Quantidade_2024, '2025', Quantidade_2025) as (Ano, Quantidade)"
)

unpivoted_stats = unpivoted_stats.withColumn("Quantidade", col("Quantidade").try_cast("int"))

unpivoted_stats.fillna(0)\
  .write.csv("/content/drive/MyDrive/TechChallenge 3/Diferença Região V2.csv", header=True, mode="overwrite")