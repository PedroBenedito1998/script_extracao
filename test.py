import os
import re
import glob
import pandas as pd
from datetime import datetime

def encontrar_arquivos_txt(diretorio_raiz: str, palavra_chave: str) -> list:
    """Encontra arquivos TXT contendo a palavra-chave no nome."""
    padrao_arquivo = f'*{palavra_chave}*.txt'
    return [os.path.join(root, arquivo) 
            for root, _, arquivos in os.walk(diretorio_raiz) 
            for arquivo in arquivos 
            if arquivo.endswith('.txt') and palavra_chave.lower() in arquivo.lower()]

def extrair_blocos_simulacao(conteudo: str) -> list:
    """Divide o conteúdo em blocos de simulação individuais."""
    # Padrão para identificar o início de cada bloco (para todos os tipos de qdisc)
    padrao_bloco = r'(qdisc \w+ \w+:.*?)(?=\n\nqdisc|\Z)'
    return re.findall(padrao_bloco, conteudo, re.DOTALL)

def extrair_dados_pie(bloco: str) -> dict:
    """Extrai dados específicos para qdisc pie."""
    dados = {}
    padroes = {
        'sent_bytes': r'Sent (\d+) bytes',
        'sent_pkts': r'Sent \d+ bytes (\d+) pkt',
        'dropped': r'dropped (\d+)',
        'overlimits': r'overlimits (\d+)',
        'requeues': r'requeues (\d+)',
        'backlog_bytes': r'backlog (\d+\w)',
        'backlog_pkts': r'backlog \d+\w (\d+)p',
        'prob': r'prob (\d+)',
        'delay': r'delay ([\d.]+(?:us|ms))',
        'pkts_in': r'pkts_in (\d+)',
        'pkts_overlimit': r'overlimit (\d+)',
        'pkts_dropped': r'dropped (\d+)',
        'maxq': r'maxq (\d+)',
        'ecn_mark': r'ecn_mark (\d+)',
        'target': r'target ([\d.]+ms)',
        'tupdate': r'tupdate ([\d.]+ms)',
        'alpha': r'alpha (\d+)',
        'beta': r'beta (\d+)'
    }
    for campo, padrao in padroes.items():
        match = re.search(padrao, bloco)
        if match:
            dados[campo] = match.group(1)
    return dados

def extrair_dados_codel(bloco: str) -> dict:
    """Extrai dados específicos para qdisc codel."""
    dados = {}
    padroes = {
        'sent_bytes': r'Sent (\d+) bytes',
        'sent_pkts': r'Sent \d+ bytes (\d+) pkt',
        'dropped': r'dropped (\d+)',
        'overlimits': r'overlimits (\d+)',
        'requeues': r'requeues (\d+)',
        'backlog_bytes': r'backlog (\d+\w)',
        'backlog_pkts': r'backlog \d+\w (\d+)p',
        'count': r'count (\d+)',
        'lastcount': r'lastcount (\d+)',
        'ldelay': r'ldelay ([\d.]+us)',
        'drop_next': r'drop_next ([\d.]+us)',
        'maxpacket': r'maxpacket (\d+)',
        'ecn_mark': r'ecn_mark (\d+)',
        'drop_overlimit': r'drop_overlimit (\d+)',
        'target': r'target ([\d.]+ms)',
        'interval': r'interval ([\d.]+ms)'
    }
    for campo, padrao in padroes.items():
        match = re.search(padrao, bloco)
        if match:
            dados[campo] = match.group(1)
    return dados

def extrair_dados_dualpi2(bloco: str) -> dict:
    """Extrai dados específicos para qdisc dualpi2."""
    dados = {}
    padroes = {
        'sent_bytes': r'Sent (\d+) bytes',
        'sent_pkts': r'Sent \d+ bytes (\d+) pkt',
        'dropped': r'dropped (\d+)',
        'overlimits': r'overlimits (\d+)',
        'requeues': r'requeues (\d+)',
        'backlog_bytes': r'backlog (\d+\w)',
        'backlog_pkts': r'backlog \d+\w (\d+)p',
        'prob': r'prob ([\d.]+)',
        'delay_c': r'delay_c ([\d.]+us)',
        'delay_l': r'delay_l ([\d.]+us)',
        'pkts_in_c': r'pkts_in_c (\d+)',
        'pkts_in_l': r'pkts_in_l (\d+)',
        'maxq': r'maxq (\d+)',
        'ecn_mark': r'ecn_mark (\d+)',
        'step_marks': r'step_marks (\d+)',
        'credit': r'credit (-?\d+)',
        'target': r'target ([\d.]+ms)',
        'tupdate': r'tupdate ([\d.]+ms)',
        'alpha': r'alpha ([\d.]+)',
        'beta': r'beta ([\d.]+)',
        'coupling_factor': r'coupling_factor (\d+)'
    }
    for campo, padrao in padroes.items():
        match = re.search(padrao, bloco)
        if match:
            dados[campo] = match.group(1)
    return dados

def extrair_dados_fq_codel(bloco: str) -> dict:
    """Extrai dados específicos para qdisc fq_codel."""
    dados = {}
    padroes = {
        'sent_bytes': r'Sent (\d+) bytes',
        'sent_pkts': r'Sent \d+ bytes (\d+) pkt',
        'dropped': r'dropped (\d+)',
        'overlimits': r'overlimits (\d+)',
        'requeues': r'requeues (\d+)',
        'backlog_bytes': r'backlog (\d+\w)',
        'backlog_pkts': r'backlog \d+\w (\d+)p',
        'maxpacket': r'maxpacket (\d+)',
        'drop_overlimit': r'drop_overlimit (\d+)',
        'new_flow_count': r'new_flow_count (\d+)',
        'ecn_mark': r'ecn_mark (\d+)',
        'new_flows_len': r'new_flows_len (\d+)',
        'old_flows_len': r'old_flows_len (\d+)',
        'target': r'target ([\d.]+ms)',
        'interval': r'interval ([\d.]+ms)',
        'quantum': r'quantum (\d+)',
        'memory_limit': r'memory_limit (\d+\w+)',
        'drop_batch': r'drop_batch (\d+)'
    }
    for campo, padrao in padroes.items():
        match = re.search(padrao, bloco)
        if match:
            dados[campo] = match.group(1)
    return dados

def extrair_dados_bloco(bloco: str, arquivo_origem: str, indice: int) -> dict:
    """Extrai dados de um bloco de simulação, identificando o tipo de qdisc."""
    # Identifica o tipo de qdisc
    tipo_match = re.match(r'qdisc (\w+)', bloco)
    if not tipo_match:
        return None
    
    tipo_qdisc = tipo_match.group(1).lower()
    dados = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'arquivo_origem': arquivo_origem,
        'simulacao_id': indice + 1,
        'qdisc_type': tipo_qdisc
    }
    
    # Extrai dados específicos para cada tipo de qdisc
    if tipo_qdisc == 'pie':
        dados.update(extrair_dados_pie(bloco))
    elif tipo_qdisc == 'codel':
        dados.update(extrair_dados_codel(bloco))
    elif tipo_qdisc == 'dualpi2':
        dados.update(extrair_dados_dualpi2(bloco))
    elif tipo_qdisc == 'fq_codel':
        dados.update(extrair_dados_fq_codel(bloco))
    else:
        print(f"Tipo de qdisc não suportado: {tipo_qdisc}")
        return None
    
    return dados

def processar_arquivo(caminho_arquivo: str) -> list:
    """Processa um arquivo completo e retorna todos os blocos de dados."""
    with open(caminho_arquivo, 'r') as f:
        conteudo = f.read()
        blocos = extrair_blocos_simulacao(conteudo)
        nome_arquivo = os.path.basename(caminho_arquivo)
        resultados = []
        
        for i, bloco in enumerate(blocos):
            dados = extrair_dados_bloco(bloco, nome_arquivo, i)
            if dados:
                resultados.append(dados)
        
        return resultados

def gerar_dataframe(dados_coletados: list) -> pd.DataFrame:
    """Cria um DataFrame pandas com os dados coletados."""
    return pd.DataFrame(dados_coletados)

def main():
    print("=== Script de Extração de Dados de Vários Tipos de Qdisc ===")
    
    diretorio = input("Diretório raiz: ").strip()
    palavra_chave = input("Palavra-chave para buscar nos arquivos: ").strip()
    
    if not os.path.isdir(diretorio):
        print("Diretório inválido!")
        return
    
    arquivos = encontrar_arquivos_txt(diretorio, palavra_chave)
    if not arquivos:
        print("Nenhum arquivo encontrado!")
        return
    
    print(f"\n{len(arquivos)} arquivo(s) encontrado(s):")
    for arq in arquivos:
        print(f"- {arq}")
    
    todos_dados = []
    for arquivo in arquivos:
        dados = processar_arquivo(arquivo)
        todos_dados.extend(dados)
        print(f"Processado: {arquivo} - {len(dados)} blocos")
    
    if not todos_dados:
        print("Nenhum dado para exportar!")
        return
    
    df = pd.DataFrame(todos_dados)
    
    # Reorganiza as colunas para colocar o tipo de qdisc no início
    colunas = ['timestamp', 'arquivo_origem', 'simulacao_id', 'qdisc_type'] + [c for c in df.columns if c not in ['timestamp', 'arquivo_origem', 'simulacao_id', 'qdisc_type']]
    df = df[colunas]
    
    csv_path = os.path.join(diretorio, f"resultados_qdisc_{palavra_chave}.csv")
    df.to_csv(csv_path, index=False)
    
    print(f"\nCSV gerado com sucesso: {csv_path}")
    print(f"Total de simulações processadas: {len(df)}")
    print(f"Tipos de qdisc encontrados: {df['qdisc_type'].unique()}")

if __name__ == "__main__":
    main()