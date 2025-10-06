import os, time, json, re, random
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from google import genai
from string import Template

# CONFIG BÁSICA
GENAI_API_KEY = os.getenv("GENAI_API_KEY", "").strip()
assert GENAI_API_KEY, "Defina o secret GENAI_API_KEY."

MODEL_NAME = os.getenv("GENAI_MODEL", "gemini-2.5-flash")

SPREADSHEET_ID_CLIENTES = os.getenv("SPREADSHEET_ID_CLIENTES", "").strip()
assert SPREADSHEET_ID_CLIENTES, "Defina o secret SPREADSHEET_ID_CLIENTES."

CREDENTIALS_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")

EMENTA_COL    = "Ementa"
OUT_ALINH_COL = "Alinhamento"
OUT_JUST_COL  = "Justificativa"

# Ajustes finos
BATCH_SIZE = int(os.getenv("ALIGN_BATCH_SIZE", "20"))
SLEEP_SEC  = float(os.getenv("ALIGN_SLEEP_SEC", "0"))   
READ_RANGE = os.getenv("ALIGN_READ_RANGE", "")          

# MAPA: ABA -> (NOME, DESCRIÇÃO)
ORG_MAP = {
    "IU": ("Instituto Unibanco (IU)",
           "O Instituto Unibanco (IU) é uma organização sem fins lucrativos que apoia redes estaduais de ensino na melhoria da gestão educacional por meio de projetos como o Jovem de Futuro, produção de conhecimento e apoio técnico a secretarias de educação."),
    "FMCSV": ("Fundação Maria Cecilia Souto Vidigal (FMCSV)",
              "A Fundação Maria Cecilia Souto Vidigal (FMCSV) atua pela causa da primeira infância no Brasil, conectando pesquisa, advocacy e apoio a políticas públicas para garantir o desenvolvimento integral de crianças de 0 a 6 anos; iniciativas como o “Primeira Infância Primeiro” oferecem dados e ferramentas para gestores e candidatos."),
    "IEPS": ("Instituto de Estudos para Políticas de Saúde (IEPS)",
             "O Instituto de Estudos para Políticas de Saúde (IEPS) é uma organização independente e sem fins lucrativos dedicada a aprimorar políticas de saúde no Brasil, combinando pesquisa aplicada, produção de evidências e advocacy em temas como atenção primária, saúde digital e financiamento do SUS."),
    "IAS": ("Instituto Ayrton Senna (IAS)",
            "O Instituto Ayrton Senna (IAS) é um centro de inovação em educação que atua em pesquisa e desenvolvimento, disseminação em larga escala e influência em políticas públicas, com foco em aprendizagem acadêmica e competências socioemocionais na rede pública."),
    "ISG": ("Instituto Sonho Grande (ISG)",
            "O Instituto Sonho Grande (ISG) é uma organização sem fins lucrativos e apartidária voltada à expansão e qualificação do ensino médio integral em redes públicas; trabalha em parceria com estados para revisão curricular, formação de equipes e gestão orientada a resultados."),
    "Reúna": ("Instituto Reúna",
              "O Instituto Reúna desenvolve pesquisas e ferramentas para apoiar redes e escolas na implementação de políticas educacionais alinhadas à BNCC, com foco em currículo, materiais de apoio e formação de professores."),
    "Reuna": ("Instituto Reúna",
              "O Instituto Reúna desenvolve pesquisas e ferramentas para apoiar redes e escolas na implementação de políticas educacionais alinhadas à BNCC, com foco em currículo, materiais de apoio e formação de professores."),
    "REMS": ("REMS – Rede Esporte pela Mudança Social",
             "A REMS – Rede Esporte pela Mudança Social articula organizações que usam o esporte como vetor de desenvolvimento humano, mobilizando atores e produzindo conhecimento para ampliar o impacto social dessa agenda no país."),
    "Manual": ("Manual (saúde)",
               "A Manual (saúde) é uma plataforma digital voltada principalmente à saúde masculina, oferecendo atendimento online e tratamentos baseados em evidências (como saúde capilar, sono e saúde sexual), com prescrição médica e acompanhamento remoto."),
    "Cactus": ("Instituto Cactus",
               "O Instituto Cactus é uma entidade filantrópica e de direitos humanos que atua de forma independente em saúde mental, priorizando adolescentes e mulheres, por meio de advocacy e fomento a projetos de prevenção e promoção de cuidado em saúde mental."),
    "Vital Strategies": ("Vital Strategies",
                         "A Vital Strategies é uma organização global de saúde pública que trabalha com governos e sociedade civil na concepção e implementação de políticas baseadas em evidências em áreas como doenças crônicas, segurança viária, qualidade do ar, dados vitais e comunicação de risco."),
    "Mevo": ("Mevo",
             "A Mevo é uma healthtech brasileira que integra soluções de saúde digital (da prescrição eletrônica à compra/entrega de medicamentos) conectando médicos, hospitais, farmácias e pacientes para tornar o cuidado mais simples e rastreável."),
    "Coletivo Feminista": ("Coletivo Feminista",
             "O Nem Presa Nem Morta (NPNM) é um movimento feminista que atua pela descriminalização e legalização do aborto no Brasil, articulando pesquisa, incidência política e mobilização social. Seus princípios ético-políticos abrangem a comunicação como direito e fundamento da democracia, a defesa do Estado democrático de direito, a compreensão de que maternidade não é dever e deve respeitar a liberdade de escolha, a promoção de uma atenção universal, equânime e integral à saúde — com ênfase no papel do SUS, no acesso a métodos contraceptivos e abortivos seguros e no respeito à autodeterminação reprodutiva —, além da defesa da descriminalização e legalização do aborto."),
}

# PROMPT
PROMPT = Template("""
Você é analista de políticas públicas e deve avaliar a coerência de uma proposição legislativa com a missão do(a) $cliente.

Missão e escopo do cliente:
$cliente_descricao

Instruções:
- Baseie-se exclusivamente no texto da **ementa** fornecida.
- Classifique o alinhamento em um dos três valores:
  • "Alinha": a ementa é compatível com os objetivos centrais do cliente.
  • "Parcial": a ementa sugere possível convergência, mas carece de detalhes para confirmação.
  • "Não Alinha": a ementa contraria ou se afasta dos objetivos do cliente.
- Produza uma justificativa breve (1 a 3 frases), clara e objetiva, fundamentada no texto da ementa.
- Responda somente em JSON válido, sem comentários ou texto extra.

Formato esperado:
{
  "alinhamento": "Alinha" | "Parcial" | "Não Alinha",
  "justificativa": "texto explicativo baseado na ementa"
}

Ementa:
\"\"\"$ementa\"\"\" 
""".strip())

# Conexões
genai_client = genai.Client(api_key=GENAI_API_KEY)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(CREDENTIALS_JSON, scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID_CLIENTES)

# Utilitários
def read_sheet_df(ws, read_range: str = "") -> pd.DataFrame:
    """Lê a aba com 1 chamada (range ou all_values). Tenta backoff p/ 429."""
    def _once():
        values = ws.get(read_range) if read_range else ws.get_all_values()
        if not values:
            return pd.DataFrame()
        header, data = values[0], values[1:]
        width = len(header)
        data = [row + [""] * (width - len(row)) for row in data]
        df = pd.DataFrame(data, columns=[h.strip() for h in header])
        return df

    delay = 1.0
    for _ in range(6):
        try:
            return _once()
        except gspread.exceptions.APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                time.sleep(delay + random.random()*0.25)
                delay = min(delay*2, 20)
                continue
            raise
    return _once()

def classify_ementa(ementa: str, nome_cli: str, desc_cli: str) -> dict:
    """Chama o Gemini e retorna dict {'alinhamento', 'justificativa'}."""
    if not ementa or not str(ementa).strip():
        return {"alinhamento": "Parcial",
                "justificativa": "Ementa ausente ou vazia; não é possível concluir o alinhamento."}

    prompt_text = PROMPT.substitute(cliente=nome_cli, cliente_descricao=desc_cli, ementa=ementa)

    stream = genai_client.models.generate_content_stream(
        model=MODEL_NAME,
        contents=prompt_text,
        config={"response_mime_type": "application/json"},
    )
    raw = "".join((chunk.text or "") for chunk in stream).strip()

    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        return {"alinhamento": "Parcial",
                "justificativa": "Saída do modelo sem JSON válido; revisão manual recomendada."}
    try:
        data = json.loads(m.group(0))
        alinh = str(data.get("alinhamento", "")).strip()
        just  = str(data.get("justificativa", "")).strip()
        if alinh not in ("Alinha", "Parcial", "Não Alinha"):
            alinh = "Parcial"
        if not just:
            just = "Sem justificativa; revisar."
        return {"alinhamento": alinh, "justificativa": just}
    except Exception:
        return {"alinhamento": "Parcial",
                "justificativa": "Falha ao interpretar JSON; revisão manual recomendada."}

def process_sheet(ws):
    """Processa uma aba: lê, classifica e grava por lote."""
    title = ws.title.strip()
    if title.lower() == "giro de notícias":
        print(f"⏭️  Pulando aba '{title}'.")
        return

    nome_cli, desc_cli = ORG_MAP.get(title, (title, ""))

    print(f"\n▶️  Aba: {title} | Cliente: {nome_cli}")

    df = read_sheet_df(ws, READ_RANGE)
    if df.empty:
        print(f"[{title}] vazia ou fora do range — pulando.")
        return

    # normaliza nomes das colunas
    df.columns = [c.strip() for c in df.columns]

    # garante colunas de saída
    if OUT_ALINH_COL not in df.columns:
        df[OUT_ALINH_COL] = ""
    if OUT_JUST_COL not in df.columns:
        df[OUT_JUST_COL] = ""

    if EMENTA_COL not in df.columns:
        print(f"[{title}] coluna '{EMENTA_COL}' não encontrada — pulando.")
        return

    # escolhe linhas a processar
    to_process = [i for i in range(len(df))
                  if not str(df.at[i, OUT_ALINH_COL]).strip()
                  and str(df.at[i, EMENTA_COL]).strip()]

    print(f"[{title}] linhas para classificar: {len(to_process)}")
    if not to_process:
        return

    for start in range(0, len(to_process), BATCH_SIZE):
        batch_idx = to_process[start:start+BATCH_SIZE]
        for i in batch_idx:
            res = classify_ementa(df.at[i, EMENTA_COL], nome_cli, desc_cli)
            df.at[i, OUT_ALINH_COL] = res["alinhamento"]
            df.at[i, OUT_JUST_COL]  = res["justificativa"]
            if SLEEP_SEC:
                time.sleep(SLEEP_SEC)

        # grava até a última linha do lote processado
        set_with_dataframe(ws, df.iloc[:max(batch_idx)+1],
                           include_index=False, include_column_header=True, resize=False)
        print(f"[{title}] 💾 salvo linhas até {max(batch_idx)+2}")

def main():
    worksheets = sh.worksheets()
    if not worksheets:
        print("Planilha sem abas.")
        return

    # percorre todas as abas, exceto a última
    for ws in worksheets[:-1]:
        process_sheet(ws)

    print("\n✅ Concluído (todas as abas exceto a última).")

if __name__ == "__main__":
    main()
