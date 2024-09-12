import pandas as pd
from sqlalchemy import create_engine

# Função para extrair dados de uma planilha
def extrair_dados(arquivo_excel):
    try:
        # Código que lê o arquivo da planilha
        dados = pd.read_excel(arquivo_excel)
        print("Dados extraídos com sucesso!")
        return dados
    except Exception as e:
        print(f"Erro ao extrair os dados: {e}")
        return None

# Função para validar o formato das colunas
def validar_formato(dados):
    colunas_esperadas = ['ESTAB', 'PESSOADOC', 'IDREPRESENTANTE']
    
    # Verifica se as colunas estão corretas
    if list(dados.columns) == colunas_esperadas:
        print("Formato das colunas validado com sucesso!")
        return True
    else:
        print(f"Erro: As colunas do arquivo não correspondem ao formato esperado. Colunas esperadas: {colunas_esperadas}")
        return False

# Função para transformar os dados (aqui pode incluir qualquer manipulação necessária)
def transformar_dados(dados):
    # Exemplo: Remover linhas nulas
    dados_transformados = dados.dropna()  # Removendo linhas com valores nulos
    print("Dados transformados com sucesso!")
    return dados_transformados

# Função para carregar os dados no banco de dados (usando SQLite neste exemplo)
def carregar_dados_no_bd(dados, tabela_bd, conexao_bd):
    try:
        # Conectar ao banco de dados
        engine = create_engine(conexao_bd)

        # Carregar os dados transformados no banco
        dados.to_sql(tabela_bd, con=engine, if_exists='replace', index=False)
        print(f"Dados carregados com sucesso na tabela '{tabela_bd}'!")
    except Exception as e:
        print(f"Erro ao carregar os dados no banco de dados: {e}")

# Função principal para executar o processo ETL
def processo_etl(arquivo_excel, tabela_bd, conexao_bd):
    # Extração
    dados_extraidos = extrair_dados(arquivo_excel)
    
    if dados_extraidos is not None:
        # Validação de formato
        if validar_formato(dados_extraidos):
            # Transformação
            dados_transformados = transformar_dados(dados_extraidos)

            # Carregamento
            carregar_dados_no_bd(dados_transformados, tabela_bd, conexao_bd)
        else:
            print("O processo ETL foi interrompido devido a erro no formato das colunas.")
    else:
        print("O processo ETL foi interrompido devido a erro na extração dos dados.")

# Definir o caminho do arquivo Excel e os detalhes do banco de dados
arquivo_excel = 'caminhoDaPasta.xlsx'  # Caminho do arquivo Excel, informações sensíveis removidas devido ao LGPD da empresa Querodiesel conforme o ano de 2024
tabela_bd = 'tabela_dados'    # Nome da tabela no banco de dados, informações sensíveis removidas devido ao LGPD da empresa Querodiesel conforme o ano de 2024
conexao_bd = 'sqlite:///meu_banco.db'  # Conexão para um banco de dado, informações sensíveis removidas devido ao LGPD da empresa Querodiesel conforme o ano de 2024

# Executar o processo ETL
processo_etl(arquivo_excel, tabela_bd, conexao_bd)
