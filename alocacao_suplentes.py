import pandas as pd
import pulp as pl
import math
import warnings
import unicodedata
import pandas as pd
import os
import os.path
from datetime import datetime
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from google.oauth2.credentials import Credentials as UserCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

warnings.filterwarnings("ignore")
# ==============================================================================
# 1) CONFIGURAÇÕES
# ==============================================================================

SPREADSHEET_ID = '1I-mD7tLgo7_ZrPf58MEeZ-bRm0cltKu2_-XssMJcRsI'
ABA_PESSOAS = "Pessoas"
ABA_PROJETOS = "Projetos"
ABA_MOBILIZADOS = "Mobilizados"
ABA_REQUISITOS = "Relatório de Flexibilidade"
ABA_DOCS_PESSOAS = "Documentos"
ABA_LOCAL = "Local"
ABA_AFASTAMENTO = "Afastamento"

DRIVE_FOLDER_ID = '1cPTWvKhWV8GqGENRoWJwEwK0i2R50j6X' 
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive"
]

TAXA_AUXILIAR = 0.16
TAXA_PADRAO = 0.08

DISTANCIA_LIMITE_KM = 200  # mantém para relatório/regra (não entra na FO agora)
PESO_DOCS = 1
PESO_DIST = 3.5
# Pesos para AUXILIAR
PESO_USO_PESSOA_AUXILIAR = 1440*4
# Pesos para ENCARREGADO
PESO_USO_PESSOA_ENCARREGADO = 1997*4
# Pesos para SONDADOR
PESO_USO_PESSOA_SONDADOR = 2129*4


# ==============================================================================
# 2) FUNÇÕES UTILITÁRIAS
# ==============================================================================

def normalizar_texto(texto):
    if pd.isna(texto) or texto is None:
        return ""
    if not isinstance(texto, str):
        texto = str(texto)
    nfkd = unicodedata.normalize("NFKD", texto)
    sem_acento = "".join([c for c in nfkd if not unicodedata.combining(c)])
    return sem_acento.upper().strip()

def limpar_id(valor):
    if pd.isna(valor) or str(valor).strip() == "": return None
    s = str(valor).strip().upper()
    if s.endswith('.0'): s = s[:-2]
    return s

def limpar_coord(valor):
    if pd.isna(valor) or str(valor).strip() == "": return 0.0
    s = str(valor).strip().replace(',', '.')
    try:
        return float(s)
    except:
        return 0.0

def classificar_cargo_padrao(texto_original):
    t = normalizar_texto(str(texto_original))
        
    if "AUXILIAR" in t and "SONDAGEM" in t:
        return "Qtd Auxiliar"
    if "ENCARREGADO DE CAMPO" in t or ("TECNICO" in t and "GEOTECNIA ESPECIALIZADO" in t):
        return "Qtd.Encarregado"
    if "SONDADOR" in t or ("OPERADOR" in t and "ENSAIOS" in t):
        return "Qtd.Sondador"
    return "Nao Identificado"

def haversine(lat1, lon1, lat2, lon2):
    if pd.isna([lat1, lon1, lat2, lon2]).any():
        return 0.0
    if (lat1 == 0 and lon1 == 0) or (lat2 == 0 and lon2 == 0):
        return 0.0
    R = 6371.0 * 1.7
    a = (math.sin(math.radians(lat2 - lat1) / 2.0) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(math.radians(lon2 - lon1) / 2.0) ** 2)
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c

def taxa_por_categoria(cat):
    return TAXA_AUXILIAR if cat == "Qtd Auxiliar" else TAXA_PADRAO

def max_mobs_por_pessoa(cat):
    t = taxa_por_categoria(cat)
    if t <= 0:
        return 0
    return max(0, int(math.floor(1.0 / t)))

def contar_detalhado_efetivos_e_desvios(df_mob, nomes_ignorar=set()):
    contagem_corretos = {"Qtd Auxiliar": 0, "Qtd.Sondador": 0, "Qtd.Encarregado": 0}
    contagem_desvios = {"Qtd Auxiliar": 0, "Qtd.Sondador": 0, "Qtd.Encarregado": 0} 
    contagem_enviados = {"Qtd Auxiliar": 0, "Qtd.Sondador": 0, "Qtd.Encarregado": 0} 
    
    if df_mob.empty:
        return contagem_corretos, contagem_desvios, contagem_enviados

    df = df_mob.copy()
    # Normaliza colunas
    df.columns = [normalizar_texto(c) for c in df.columns]
    
    col_status = next((c for c in df.columns if "STATUS" in c), None)
    col_cargo = next((c for c in df.columns if "CARGO" in c), None)
    col_nome = next((c for c in df.columns if "NOME" in c), None)
    
    if not col_status or not col_cargo:
        print(f" [AVISO] Colunas Status/Cargo não encontradas. Colunas: {df.columns.tolist()}")
        return contagem_corretos, contagem_desvios, contagem_enviados

    for _, row in df.iterrows():
        # 1. Filtro da Vale Norte (pula quem está na lista)
        if col_nome and nomes_ignorar:
            if normalizar_texto(row[col_nome]) in nomes_ignorar:
                continue 

        status_raw = normalizar_texto(row[col_status])
        
        if "EFETIVO" in status_raw:
            cargo_raw = normalizar_texto(row[col_cargo])
            
            # AQUI ESTÁ O TRUQUE: Tenta classificar o Cargo Original primeiro
            cat_origem = classificar_cargo_padrao(cargo_raw)
            
            # Agora tenta descobrir o destino no Status.
            # 1. Tenta direto (ex: se o status for só "AUXILIAR")
            cat_destino = classificar_cargo_padrao(status_raw)
            
            # 2. Se falhar, LIMPA o texto tirando "EFETIVO" e "-" (ex: "EFETIVO - AUXILIAR" vira "AUXILIAR")
            if cat_destino == "Nao Identificado":
                texto_limpo = status_raw.replace("EFETIVO", "").replace("-", "").strip()
                cat_destino = classificar_cargo_padrao(texto_limpo)
            
            # 3. Se ainda falhar (ex: status é só "EFETIVO"), assume que ele continua no cargo original
            if cat_destino == "Nao Identificado":
                cat_destino = cat_origem

            # Contabiliza
            if cat_destino != "Nao Identificado" and cat_origem != "Nao Identificado":
                if cat_origem == cat_destino:
                    contagem_corretos[cat_destino] += 1
                else:
                    contagem_desvios[cat_destino] += 1
                    contagem_enviados[cat_origem] += 1 
                
    return contagem_corretos, contagem_desvios, contagem_enviados


# ==============================================================================
# 3) FUNÇÕES GOOGLE (SHEETS E DRIVE)
# ==============================================================================
def get_google_sheet_data(sheet_name):
    # Configuração de autenticação usando o arquivo credentials.json
    creds = Credentials.from_service_account_file(
        'credentials.json', 
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )

    # Cria o serviço para interagir com a API do Google Sheets
    service = build('sheets', 'v4', credentials=creds)

    try:
        # Acessa os dados da aba. A consulta vai até a última célula usada automaticamente.
        range_ = f"{sheet_name}!A1:Z"  # A1 até Z1000
        result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=range_).execute()

        # Verifica se os dados foram retornados corretamente
        if 'values' in result:
            valores = result['values']
            if valores:
                # Usa o cabeçalho da planilha (primeira linha) como colunas
                cabecalho = valores[0]
                
                # Aqui, convertendo a lista de listas para um DataFrame do Pandas
                df = pd.DataFrame(valores[1:], columns=cabecalho)
                
                # Se o número de colunas não for consistente, ajuste
                if len(df.columns) != len(cabecalho):
                    print(f"Atenção: Número de colunas inconsistente. Ajustando...")
                    df = pd.DataFrame(valores[1:])  # Sem cabeçalho, apenas os dados
                    df.columns = [f"Coluna_{i}" for i in range(len(df.columns))]  # Atribui colunas genéricas

                return df
            else:
                print(f"Erro: Não há dados na aba '{sheet_name}'.")
                return pd.DataFrame()  # Retorna DataFrame vazio se não houver dados
        else:
            print(f"Erro: Não há dados no intervalo '{range_}'.")
            return pd.DataFrame()

    except Exception as e:
        print(f"Erro ao acessar a planilha {sheet_name}: {e}")
        return pd.DataFrame()  # Retorna DataFrame vazio em caso de erro

# ------------------------------------------------------------------
# BOT (Para ler/escrever na Planilha)
# ------------------------------------------------------------------
def get_bot_creds():
    """ Usa o credentials.json (Service Account) """
    if not os.path.exists('credentials.json'):
        print("[ERRO] Arquivo 'credentials.json' do Bot não encontrado.")
        return None
    
    # Scopes básicos para o Bot mexer na planilha
    return ServiceAccountCredentials.from_service_account_file(
        'credentials.json', 
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )

# ------------------------------------------------------------------
# LOGIN PESSOAL  (Para criar arquivo no Drive)
# ------------------------------------------------------------------
def get_user_creds():
    """ Usa o client_secret.json (OAuth) para logar como USUÁRIO """
    creds = None
    if os.path.exists('token.json'):
        try:
            creds = UserCredentials.from_authorized_user_file('token.json', ["https://www.googleapis.com/auth/drive"])
        except Exception:
            creds = None
            
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists('client_secret.json'):
                print("❌ ERRO: Baixe o 'client_secret.json' (OAuth) para fazer o upload!")
                return None
                
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret.json', 
                scopes=["https://www.googleapis.com/auth/drive"] # Só precisa de Drive
            )
            creds = flow.run_local_server(port=0)
            
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
            
    return creds

def salvar_log_na_planilha(status, link_drive):
    print("   -> Registrando link na planilha...")
    creds = get_bot_creds()
    if not creds: return

    try:
        service = build('sheets', 'v4', credentials=creds)
        
        import uuid
        id_unico = str(uuid.uuid4())[:8] # Gera um ID curto 
        agora = datetime.now().strftime("%d/%m/%Y") # Formato de data da planilha
        
        valores = [[id_unico, "Otimizar Suplentes Atuais", agora, link_drive]]
        
        body = {'values': valores}
        
        NOME_DA_ABA = "Otimizar Alocação de Suplentes"
        
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{NOME_DA_ABA}'!A5", 
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        
        print(f"   Dados salvo na aba '{NOME_DA_ABA}' com sucesso!")
        
    except Exception as e:
        print(f"   ❌ Erro ao salvar dados: {e}")

def upload_file_to_drive(filename, folder_id):
    print(f"   -> Criando arquivo novo '{filename}' no Drive (usando cota de Usuário)...")
    
    creds = get_user_creds()
    if not creds: return None

    try:
        service = build('drive', 'v3', credentials=creds)
        
        file_metadata = {
            'name': filename, 
            'parents': [folder_id]
        }
        
        media = MediaFileUpload(filename, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        
        file = service.files().create(
            body=file_metadata, 
            media_body=media, 
            fields='id, webViewLink'
        ).execute()
        
        link = file.get('webViewLink')
        print(f"   ✅ Arquivo criado! Link: {link}")
        return link
        
    except Exception as e:
        print(f"   ❌ Erro no upload: {e}")
        return None
    
# ==============================================================================
# 3) MAPAS
# ==============================================================================

def carregar_matriz_requisitos(df_req):
    if df_req is None or df_req.empty:
        return {}

    df_req = df_req.copy()
    df_req.columns = [c.strip() for c in df_req.columns]

    col_proj = next((c for c in df_req.columns if "Projeto" in c), None)
    col_func = next((c for c in df_req.columns if "Função" in c or "Funcao" in c), None)
    col_doc  = next((c for c in df_req.columns if "Documento" in c), None)

    if not (col_proj and col_func and col_doc):
        return {}

    map_req = {}
    for _, row in df_req.iterrows():
        proj = normalizar_texto(row[col_proj])
        cat  = classificar_cargo_padrao(row[col_func])
        doc  = normalizar_texto(row[col_doc])

        if cat == "Nao Identificado" or doc in ("NAN", "") or proj in ("NAN", ""):
            continue
    

        map_req.setdefault(proj, {}).setdefault(cat, set()).add(doc)

    return map_req

def carregar_matriz_posse(df_docs):
    if df_docs is None or df_docs.empty:
        return {}

    df_docs = df_docs.copy()
    df_docs.columns = [c.strip() for c in df_docs.columns]

    col_status = next((c for c in df_docs.columns if "Status" in c or "Situação" in c or "Situacao" in c), None)
    col_doc    = next((c for c in df_docs.columns if "Documento" in c), None)
    col_nome   = next((c for c in df_docs.columns if "Colaborador" in c or "Nome" in c), None)

    if not (col_status and col_doc and col_nome):
        return {}

    def status_eh_seguro(val):
        s = normalizar_texto(val)
        if "VENCIDO" in s:
            return False
        if "VIGENTE" in s or "OK" in s or "APROVADO" in s:
            return True
        if "VENCE EM" in s:
            if "30" in s:
                return False
            if "60" in s:
                return True
        return False

    df_validos = df_docs[df_docs[col_status].apply(status_eh_seguro)]
    map_has = {}

    for _, row in df_validos.iterrows():
        nome = normalizar_texto(row[col_nome])
        doc  = normalizar_texto(row[col_doc])
        if nome in ("NAN", "") or doc in ("NAN", ""):
            continue
        map_has.setdefault(nome, set()).add(doc)

    return map_has

def carregar_coordenadas_maxima_distancia(df_proj, df_local):
    """
    1. Lê ID do Projeto (Col A) e Nome (Col B) da aba Projetos.
    2. Procura ID correspondente na Coluna D da aba Local.
    3. Calcula distância de BH para todas as ocorrências.
    4. Escolhe a MAIOR distância.
    """
    if df_proj.empty or df_local.empty:
        return {}

    print("   -> Calculando coordenadas (Regra: Maior distância de BH)...")

    # Coordenadas de Referência (Belo Horizonte)
    BH_LAT, BH_LON = -19.9227, -43.9451
    
    # 1. Indexar aba Local por ID
    mapa_locais_por_id = {}
    
    # Adaptação para achar colunas dinamicamente (para segurança no Excel Local)
    cols = df_local.columns
    col_lat = next((c for c in cols if "Lat" in c), df_local.columns[1] if len(cols)>1 else None)
    col_lon = next((c for c in cols if "Lon" in c), df_local.columns[2] if len(cols)>2 else None)
    col_id_local = next((c for c in cols if "ID" in c or "Centro" in c), df_local.columns[3] if len(cols)>3 else None)

    for _, row in df_local.iterrows():
        if not col_id_local: break
        
        lat_raw = row[col_lat]
        lon_raw = row[col_lon]
        id_raw = row[col_id_local]
        
        id_limpo = limpar_id(id_raw)
        lat = limpar_coord(lat_raw)
        lon = limpar_coord(lon_raw)
        
        if id_limpo and lat != 0 and lon != 0:
            if id_limpo not in mapa_locais_por_id:
                mapa_locais_por_id[id_limpo] = []
            mapa_locais_por_id[id_limpo].append((lat, lon))

    mapa_final = {} # { "Nome Projeto": (lat, lon) }

    for _, row_p in df_proj.iterrows():
        if len(row_p) < 2: continue
        
        p_id_raw = row_p.iloc[0]
        p_nome = normalizar_texto(str(row_p.iloc[1]).strip())
        
        if not p_nome: continue
        
        p_id_limpo = limpar_id(p_id_raw)
        
        candidatos = mapa_locais_por_id.get(p_id_limpo, [])
        
        if not candidatos:
            mapa_final[p_nome] = (0, 0)
            continue
            
        # Lógica da Maior Distância
        melhor_dist = -1.0
        melhor_coord = (0, 0)
        
        for lat, lon in candidatos:
            d = haversine(BH_LAT, BH_LON, lat, lon)
            if d > melhor_dist:
                melhor_dist = d
                melhor_coord = (lat, lon)
        
        mapa_final[p_nome] = melhor_coord

    return mapa_final

def carregar_minimo_por_projeto_de_projetos(df_proj, categoria):
    if df_proj is None or df_proj.empty:
        return {}

    df = df_proj.copy()
    
    # 1. Tenta achar coluna de Nome do Projeto
    found = next((c for c in df.columns if "NOME" in c.upper() and "PROJETO" in c.upper()), None)
    if found:
         df.rename(columns={found: "Nome projeto"}, inplace=True)
         
    if "Nome projeto" not in df.columns:
        return {}

    # 2. Identifica a coluna de Quantidade para o Cargo atual
    cols_norm = {normalizar_texto(c): c for c in df.columns}
    cat_norm = normalizar_texto(categoria)
    col_cat = cols_norm.get(cat_norm)

    if col_cat is None:
        # Tenta achar por aproximação (ex: "SONDADOR" dentro de "QTD SONDADOR")
        for cn, real in cols_norm.items():
            if cat_norm in cn: 
                col_cat = real
                break

    if col_cat is None:
        return {}

    teto = {}
    for _, row in df.iterrows():
        p_raw = row["Nome projeto"]
        p = normalizar_texto(str(p_raw).strip())
        
        if p in ["NAN", ""] or pd.isna(p_raw):
            continue
        # --------------------------------

        val = pd.to_numeric(row.get(col_cat, 0), errors="coerce")
        m = int(val) if not pd.isna(val) else 0
        
        if m > 0:
            teto[p] = m

    return teto

# ==============================================================================
# 4) OTIMIZAÇÃO
# ==============================================================================

def rodar_distribuicao(
     categoria,
    pessoas_reserva,
    df_proj,
    map_req,
    map_has,
    teto_projeto,
    map_lat_lon,
    meta_suplentes_calculada
):
    print(f"\n--- PROCESSANDO: {categoria.upper()} ---")

    MAX_MOBS = max_mobs_por_pessoa(categoria)
    taxa = taxa_por_categoria(categoria)

    projetos_ativos = sorted([p for p, m in teto_projeto.items() if m > 0])

    print(f"  > Reservas: {len(pessoas_reserva)} | Projetos (ativos): {len(projetos_ativos)}")
    print(f"  > Taxa: {taxa} | MAX_MOBS_POR_PESSOA: {MAX_MOBS}")

    if not projetos_ativos:
        return [], [], pessoas_reserva
    if not pessoas_reserva:
        return [], [], []

    # ---------- docs relevantes ----------
    docs_relevantes = set()
    for p in projetos_ativos:
        docs_relevantes |= map_req.get(p, {}).get(categoria, set())
    docs_relevantes = sorted(list(docs_relevantes))

    # ---------- pares, distância real e custo (para relatório apenas) ----------
    pares = []
    dist_par = {}   # (pA,pB) -> km real
    custo_par = {}  # (pA,pB) -> 200 se <=200km, senão km real (NÃO entra na FO agora)

    for i in range(len(projetos_ativos)):
        for k in range(i + 1, len(projetos_ativos)):
            pA = projetos_ativos[i]
            pB = projetos_ativos[k]
            cA = map_lat_lon.get(pA, (0, 0))
            cB = map_lat_lon.get(pB, (0, 0))

            d = haversine(cA[0], cA[1], cB[0], cB[1])
            pares.append((pA, pB))
            dist_par[(pA, pB)] = float(d)

            # regra original, só para relatório
            if d > 0 and d > DISTANCIA_LIMITE_KM:
                custo_par[(pA, pB)] = float(d)
            else:
                custo_par[(pA, pB)] = float(DISTANCIA_LIMITE_KM)

    # ---------- MODELAGEM ----------
    prob = pl.LpProblem(f"Dist_{categoria}", pl.LpMinimize)

    x = pl.LpVariable.dicts("x", (pessoas_reserva, projetos_ativos), cat="Binary")
    z = pl.LpVariable.dicts("z", pessoas_reserva, cat="Binary")
    falta = pl.LpVariable.dicts("falta", projetos_ativos, lowBound=0, cat="Integer")

    y = pl.LpVariable.dicts("y", (pessoas_reserva, docs_relevantes), cat="Binary") if docs_relevantes else None

    # w[j,par] binária: 1 <=> pessoa j pegou pA e pB
    w = pl.LpVariable.dicts("w", (pessoas_reserva, range(len(pares))), cat="Binary") if pares else None

    # (1) Mínimo por projeto com slack
    for p in projetos_ativos:
        prob += pl.lpSum(x[j][p] for j in pessoas_reserva) + falta[p] >= int(math.ceil(teto_projeto[p]/MAX_MOBS))

    # (2) Link x -> z
    for j in pessoas_reserva:
        for p in projetos_ativos:
            prob += x[j][p] <= z[j]

    prob += pl.lpSum(z[j] for j in pessoas_reserva) <= int(math.ceil(sum(teto_projeto[p] for p in projetos_ativos) * taxa))

    # (4) Documentos (relaxado via y)
    if docs_relevantes:
        for j in pessoas_reserva:
            docs_da_pessoa = map_has.get(j, set())
            for p in projetos_ativos:
                reqs = map_req.get(p, {}).get(categoria, set())
                for d in reqs:
                    has_doc = 1 if d in docs_da_pessoa else 0
                    prob += x[j][p] <= has_doc + y[j][d]

    # (5) Linearização w = AND(xA, xB)
    if pares:
        for j in pessoas_reserva:
            for idx, (pA, pB) in enumerate(pares):
                prob += w[j][idx] <= x[j][pA]
                prob += w[j][idx] <= x[j][pB]
                prob += w[j][idx] >= x[j][pA] + x[j][pB] - 1

    # (6) Regra de Agrupamento VALE
    # Se uma pessoa for alocada em um projeto VALE, deve ser alocada em TODOS os projetos VALE.
    projetos_vale = [p for p in projetos_ativos if "5211" in p]
    if len(projetos_vale) > 1:
        p_ref = projetos_vale[0]
        for j in pessoas_reserva:
            for p_other in projetos_vale[1:]:
                prob += x[j][p_ref] == x[j][p_other]


    # ---------- PESOS ESPECÍFICOS POR CARGO ----------
    if categoria == "Qtd Auxiliar":
        PESO_USO_PESSOA = PESO_USO_PESSOA_AUXILIAR
        PESO_FALTA = PESO_USO_PESSOA_AUXILIAR*(12)
        PESO_D = PESO_DIST
    elif categoria == "Qtd.Encarregado":
        PESO_USO_PESSOA = PESO_USO_PESSOA_ENCARREGADO
        PESO_FALTA = PESO_USO_PESSOA_ENCARREGADO*(12)
        PESO_D = 0 #Encarregado não precisa rodar, pois irá ser usado poucas vezes
    elif categoria == "Qtd.Sondador":
        PESO_USO_PESSOA = PESO_USO_PESSOA_SONDADOR
        PESO_FALTA = PESO_USO_PESSOA_SONDADOR*(12)
        PESO_D = PESO_DIST

    # ---------- OBJETIVO ----------
    obj = PESO_FALTA * pl.lpSum(falta[p] for p in projetos_ativos)
    obj += PESO_USO_PESSOA * pl.lpSum(z[j] for j in pessoas_reserva)
    # Soma todas as alocações 'x' em vez das pessoas em projetos:
    obj += 600 * pl.lpSum(x[j][p] for j in pessoas_reserva for p in projetos_ativos)

    if docs_relevantes:
        obj += PESO_DOCS * pl.lpSum(y[j][d] for j in pessoas_reserva for d in docs_relevantes)

    #custo de distância na FO é a distância real dist_par (km)
    if pares:
        obj += PESO_D * pl.lpSum(dist_par[pares[idx]] * w[j][idx]
                                    for j in pessoas_reserva
                                    for idx in range(len(pares)))

    prob += obj

    print("  ⏳ Otimizando...")
    try:
        solver = pl.HiGHS(msg=True,timeLimit=300)
        prob.solve(solver)
    except:
        solver = pl.PULP_CBC_CMD(msg=True, timeLimit=300)
        prob.solve(solver)

    # ---------- RESULTADOS ----------
    lista_pessoas = []
    pessoas_usadas = set()

    for j in pessoas_reserva:
        projs = [p for p in projetos_ativos if pl.value(x[j][p]) > 0.9]
        if projs:
            pessoas_usadas.add(j)

            pendencias = []
            if docs_relevantes:
                pendencias = [d for d in docs_relevantes if pl.value(y[j][d]) > 0.9]

            km_total = 0.0
            custo_regra_total = 0.0
            if pares:
                for idx in range(len(pares)):
                    if (pl.value(w[j][idx]) or 0) > 0.9:
                        par = pares[idx]
                        km_total += dist_par[par]
                        custo_regra_total += custo_par[par]

            obs = []
            if len(projs) >= MAX_MOBS:
                obs.append(f"Max({MAX_MOBS})")
            if pares:
                obs.append(f"Km={km_total:.1f}")

            obs_texto = " | ".join(obs) if obs else "OK"
            status_doc = "OK" if not pendencias else f"Falta: {', '.join(pendencias)}"

            lista_pessoas.append({
                "Cargo": categoria,
                "Nome": j,
                "Qtd Mobilizações": len(projs),
                "Distância Total (km) (FO)": round(km_total, 2),
                "Custo (regra 200/real) (relatório)": round(custo_regra_total, 2),
                "Obs": obs_texto,
                "Projetos Alocados": ", ".join(projs),
                "Status Documentação": status_doc
            })

    lista_projetos = []
    for p in projetos_ativos:
        alocado = int(round(pl.value(pl.lpSum(x[j][p] for j in pessoas_reserva)) or 0))
        faltou = int(round(pl.value(falta[p]) or 0))
        meta = int(teto_projeto[p])

        lista_projetos.append({
            "Cargo": categoria,
            "Projeto": p,
            "Min (Projetos)": meta,
            "Alocado": alocado,
            "Faltou": faltou,
            "Status": "OK" if faltou == 0 else f"FALTA ({faltou})"
        })

    sobras = [j for j in pessoas_reserva if j not in pessoas_usadas]
    return lista_pessoas, lista_projetos, sobras

# ==============================================================================
# 5) MAIN
# ==============================================================================

def main():
    print("Iniciando Análise...")

    try:
        # Acesse os dados do Google Sheets
        df_pes = get_google_sheet_data(ABA_PESSOAS)
        df_proj = get_google_sheet_data(ABA_PROJETOS)
        df_mob = get_google_sheet_data(ABA_MOBILIZADOS)
        df_req = get_google_sheet_data(ABA_REQUISITOS)
        df_docs = get_google_sheet_data(ABA_DOCS_PESSOAS)
        df_local = get_google_sheet_data(ABA_LOCAL)
        df_afastamento = get_google_sheet_data(ABA_AFASTAMENTO)
        
    except Exception as e:
        print(f"Erro ao ler Google Sheets: {e}")
        return

    # Verifique se os DataFrames estão vazios corretamente com .empty
    if df_pes.empty or df_proj.empty:
        print("Dados de Pessoas ou Projetos não encontrados.")
        return

    df_pes = df_pes.copy()
    
    col_nome_pes = next((c for c in df_pes.columns if "NOME" in c.upper()), "Nome")
    col_status_pes = next((c for c in df_pes.columns if "STATUS" in c.upper()), "Status")
    col_cargo_pes = next((c for c in df_pes.columns if "CARGO" in c.upper()), "Cargo")

    df_pes["NOME_NORM"] = df_pes[col_nome_pes].apply(normalizar_texto)
    df_pes["STATUS_NORM"] = df_pes[col_status_pes].apply(normalizar_texto)
    df_pes["CATEGORIA"] = df_pes[col_cargo_pes].apply(classificar_cargo_padrao)
    df_pes.drop_duplicates(subset=["NOME_NORM"], inplace=True)

    # Filtro de Projetos
    projs_ignorar = ["5426 VALE NORTE", "5426 VELA NORTE", "6637 APIA", "5426 NORTE VMG"]
    if not df_proj.empty:
        col_nome_proj = next((c for c in df_proj.columns if "NOME" in c.upper() and "PROJETO" in c.upper()), None)
        if col_nome_proj:
            df_proj = df_proj[~df_proj[col_nome_proj].apply(lambda x: normalizar_texto(x) in projs_ignorar)]

    map_req = carregar_matriz_requisitos(df_req)
    map_has = carregar_matriz_posse(df_docs)
    map_lat_lon = carregar_coordenadas_maxima_distancia(df_proj, df_local)

    # =========================================================================
    # LÓGICA DE BLOQUEIO E CONTAGEM
    # =========================================================================
    print("   -> Verificando Efetivos para bloqueio...")
    nomes_bloqueados = set()
    map_efetivos_corretos, map_desvios_recebidos, map_desvios_enviados = {}, {}, {}
    
    if not df_mob.empty:
        
        map_efetivos_corretos, map_desvios_recebidos, map_desvios_enviados = contar_detalhado_efetivos_e_desvios(df_mob)
        
        df_mob2 = df_mob.copy()
        df_mob2.columns = [normalizar_texto(c) for c in df_mob2.columns]
        col_nome_mob = next((c for c in df_mob2.columns if "NOME" in c or "COLABORADOR" in c), None)
        col_status_mob = next((c for c in df_mob2.columns if "STATUS" in c or "SITUACAO" in c), None)
        col_proj_mob = next((c for c in df_mob2.columns if "PROJETO" in c or "CENTRO" in c), None)

        if col_nome_mob and col_status_mob:
            pessoas_mob = {}

            # 1. Agrupa os dados
            for _, row in df_mob2.iterrows():
                nome = normalizar_texto(row[col_nome_mob])
                if not nome: continue
                status = normalizar_texto(row[col_status_mob])
                # Pega o projeto se existir, senão vazio
                proj_mob = normalizar_texto(row[col_proj_mob]) if col_proj_mob else ""

                # Ignora quem não está em campo para fins de histórico
                if any(k in status for k in ["DISPONIVEL", "DISPONÍVEL", "AGUARDANDO", "BASE"]) and "EM CAMPO" not in status:
                    continue

                if nome not in pessoas_mob: pessoas_mob[nome] = []
                pessoas_mob[nome].append({'proj': proj_mob, 'status': status})

            # =================================================================
            # Regras (Vale Norte )
            # =================================================================
            nomes_ignorar_grafico = set()
            
            # Lista 2: Quem eu bloqueio de novos projetos? -> TODO MUNDO OCUPADO
            nomes_bloqueados = set() 

            for nome, registros in pessoas_mob.items():
                # --- Verifica as condições da pessoa ---
                eh_vale_norte = False
                eh_efetivo_qualquer = False
                
                for r in registros:
                    if "EFETIVO" in r['status']:
                        eh_efetivo_qualquer = True
                    
                    # Verifica se é projeto proibido (Vale Norte, etc)
                    if any(p in r['proj'] for p in projs_ignorar):
                        eh_vale_norte = True

                # --- PREENCHE LISTA 1: GRAFICOS (Só ignora se for Vale Norte) ---
                if eh_vale_norte:
                    nomes_ignorar_grafico.add(nome)

                # --- PREENCHE LISTA 2: BLOQUEIO (Ignora se for Efetivo OU Vale Norte) ---
                # Se a pessoa já é efetiva em qualquer lugar, bloqueia ela de novos projetos
                if eh_efetivo_qualquer or eh_vale_norte:
                    nomes_bloqueados.add(nome)

    # =========================================================================
    #  FILTRAR PESSOAS COM AFASTAMENTO "SAUDE"
    # =========================================================================
    print("   -> Verificando Afastamentos (Saúde) para bloqueio...")
    if not df_afastamento.empty:
        df_afast = df_afastamento.copy()
        df_afast.columns = [normalizar_texto(c) for c in df_afast.columns]
    # Procura as colunas de Nome e Razão
        col_nome_afast = next((c for c in df_afast.columns if "NOME" in c), None)
        col_razao = next((c for c in df_afast.columns if "RAZAO" in c or "MOTIVO" in c), None)
        
        if col_nome_afast and col_razao:
            count_saude = 0
            for _, row in df_afast.iterrows():
                razao_raw = normalizar_texto(row[col_razao])
                
                if "SAUDE" in razao_raw:
                    nome_afastado = normalizar_texto(row[col_nome_afast])
                    if nome_afastado:
                        nomes_bloqueados.add(nome_afastado)
                        count_saude += 1
            print(f"      [INFO] {count_saude} pessoas bloqueadas por motivo de Saúde.")
    # =========================================================================

    if not df_mob.empty:
        # AQUI O SEGREDO: Passamos apenas a lista da Vale Norte para ser ignorada na contagem
        # Assim, os efetivos dos outros projetos SERÃO CONTADOS!
        map_efetivos_corretos, map_desvios_recebidos, map_desvios_enviados = contar_detalhado_efetivos_e_desvios(
            df_mob, 
            nomes_ignorar=nomes_ignorar_grafico
        )
    else:
        map_efetivos_corretos, map_desvios_recebidos, map_desvios_enviados = {}, {}, {}

    print(f"\n- Efetivos Corretos: {map_efetivos_corretos}")
    print(f"- Desvios Recebidos: {map_desvios_recebidos}")
    print(f"- Desvios Enviados (Custo): {map_desvios_enviados}")

    df_disponiveis = df_pes[df_pes["STATUS_NORM"] == "ATIVO"].copy()
    reservas_validas = df_disponiveis[~df_disponiveis["NOME_NORM"].isin(nomes_bloqueados)].copy()

    grupos = ["Qtd Auxiliar", "Qtd.Sondador", "Qtd.Encarregado"]
    res_pes, res_proj, res_sobras = [], [], []
    resumo_gerencial = []

    for cat in grupos:
        pessoas_cat = reservas_validas[reservas_validas["CATEGORIA"] == cat]["NOME_NORM"].tolist()
        teto_proj = carregar_minimo_por_projeto_de_projetos(df_proj, cat)

        if sum(teto_proj.values()) == 0: continue

        # 1) Demanda Projetos
        demanda_projetos = sum(teto_proj.values())
        
        # Recupera contagens
        qtd_corretos = map_efetivos_corretos.get(cat, 0)
        qtd_recebidos = map_desvios_recebidos.get(cat, 0)
        qtd_enviados = map_desvios_enviados.get(cat, 0) # Recupera quem saiu
        
        # 2) Efetivos Alocados (Exatos)
        efetivos_alocados_exatos = qtd_corretos

        # Total Operacional (Corretos + Quem veio de fora)
        total_ocupados_operacao = qtd_corretos + qtd_recebidos
        
        # 4) Diferença Operacional
        gap_operacional = demanda_projetos - total_ocupados_operacao

        # --- OTIMIZAÇÃO E SUPLÊNCIA ---
        taxa = taxa_por_categoria(cat) 
        qtd_permitida_suplentes = int(math.ceil(demanda_projetos * taxa)) # 5) Meta

        rp, rj, sobras = rodar_distribuicao(
            categoria=cat,
            pessoas_reserva=pessoas_cat,
            df_proj=df_proj,
            map_req=map_req,
            map_has=map_has,
            teto_projeto=teto_proj,
            map_lat_lon=map_lat_lon,
            meta_suplentes_calculada=qtd_permitida_suplentes 
        )

        qtd_alocados_real = sum(1 for p in rp if p['Qtd Mobilizações'] > 0)
        qtd_total_banco_suplentes = len(pessoas_cat)
        
        # Gap de Suplência (Meta - Real Banco)
        gap_suplencia = qtd_permitida_suplentes - qtd_total_banco_suplentes
        
        # ---------------------------------------------------------------------
        # LÓGICA FINANCEIRA (COM PENALIDADE DE DESVIO ENVIADO)
        # ---------------------------------------------------------------------
        # Ignora quem veio de fora (recebidos) para o cálculo financeiro base
        gap_financeiro_efetivos = demanda_projetos - qtd_corretos 

        # 8) Folga/Excedentes (SALDO TOTAL FINANCEIRO)
        # Saldo = (Gap Efetivos + Gap Suplencia) - Desvios Enviados
        saldo_consolidado_financeiro = gap_financeiro_efetivos + gap_suplencia - qtd_enviados
        
        # 9) Impacto Financeiro
        custo_unit = 0
        if "AUXILIAR" in cat.upper(): custo_unit = PESO_USO_PESSOA_AUXILIAR
        elif "ENCARREGADO" in cat.upper(): custo_unit = PESO_USO_PESSOA_ENCARREGADO
        elif "SONDADOR" in cat.upper(): custo_unit = PESO_USO_PESSOA_SONDADOR
        
        impacto_financeiro = saldo_consolidado_financeiro * custo_unit
        
        cenario_txt = "Economia" if impacto_financeiro >= 0 else "Custo Extra"

        resumo_gerencial.append({
            "Cargo": cat,
            "Demanda Projetos": demanda_projetos,
            "Efetivos Alocados": efetivos_alocados_exatos, 
            "Desvio de Função": qtd_recebidos,               
            "Diferença Demanda x Efetivos": gap_operacional, 
            "Meta de Suplentes": qtd_permitida_suplentes,
            "Pessoas disponíveis para suplência": qtd_total_banco_suplentes,
            "Suplentes alocados": qtd_alocados_real,
            "Folga/Excedentes": saldo_consolidado_financeiro, 
            "Impacto Financeiro": impacto_financeiro,         
            "Cenário": cenario_txt
        })
        
        res_pes.extend(rp)
        res_proj.extend(rj)
        for nome in sobras:
            res_sobras.append({"Cargo": cat, "Nome": nome, "Status": "Excedente (Disponível)"})

    if res_pes or res_proj or res_sobras:
        out = "Resultado_Alocacao_Suplentes.xlsx"
        try:
            with pd.ExcelWriter(out) as writer:
                if res_pes:
                    pd.DataFrame(res_pes).sort_values(["Cargo", "Qtd Mobilizações"], ascending=False)\
                        .to_excel(writer, sheet_name="Alocação Sugerida", index=False)
                if res_proj:
                    pd.DataFrame(res_proj).sort_values(["Cargo", "Projeto"])\
                        .to_excel(writer, sheet_name="Status Metas", index=False)
                if res_sobras:
                    pd.DataFrame(res_sobras).sort_values(["Cargo", "Nome"])\
                        .to_excel(writer, sheet_name="Banco de Excedentes", index=False)

                if resumo_gerencial:
                    df_res = pd.DataFrame(resumo_gerencial)
                    
                    cols_fin = [
                        "Cargo", 
                        "Demanda Projetos", 
                        "Efetivos Alocados", 
                        "Desvio de Função", 
                        "Diferença Demanda x Efetivos", 
                        "Meta de Suplentes", 
                        "Pessoas disponíveis para suplência", 
                        "Suplentes alocados", 
                        "Folga/Excedentes", 
                        "Impacto Financeiro", 
                        "Cenário"
                    ]
                    df_res[cols_fin].to_excel(writer, sheet_name="Resumo Gerencial", index=False)
        
            print(f"\n[SUCESSO] Relatório Gerado: {out}")
            
            link = upload_file_to_drive(out, DRIVE_FOLDER_ID)
            
            if link:
                salvar_log_na_planilha("Sucesso", link)

        except PermissionError:
            print(f"\n[ERRO] Não foi possível salvar '{out}'. Feche o arquivo se ele estiver aberto.")

if __name__ == "__main__":
    main()