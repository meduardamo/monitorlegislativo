import os, re, time, requests, pandas as pd
from datetime import datetime
from urllib.parse import urlparse

# ---------- Timezone BR ----------
try:
    from zoneinfo import ZoneInfo
except Exception:
    from backports.zoneinfo import ZoneInfo  # type: ignore

TZ_BR = ZoneInfo("America/Sao_Paulo")
now_br = lambda: datetime.now(TZ_BR)
today_iso = lambda: now_br().date().strftime("%Y-%m-%d")
today_compact = lambda: now_br().date().strftime("%Y%m%d")

# ---------- HTTP ----------
HDR = {
    "Accept":"application/json,text/html,*/*",
    "User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
}

def _as_list(x):
    if x is None: return []
    return x if isinstance(x, list) else [x]

def _dig(d: dict, path, default=None):
    cur = d
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur

# ====================== SENADO ======================
from bs4 import BeautifulSoup
BASE_PESQUISA_SF = "https://legis.senado.leg.br/dadosabertos/materia/pesquisa/lista.json"

def _senado_inteiro_teor_url_from_api(codigo_materia: int|str):
    tries = [
        f"https://legis.senado.leg.br/dadosabertos/materia/textos/{codigo_materia}.json",
        f"https://legis.senado.leg.br/dadosabertos/materia/{codigo_materia}/textos.json",
    ]
    for u in tries:
        r = requests.get(u, timeout=30, headers=HDR)
        if r.status_code != 200: continue
        j = r.json()
        textos = (_dig(j, ("TextoMateria","Textos","Texto"))
                  or _dig(j, ("Textos","Texto"))
                  or j.get("Textos") or [])
        textos = _as_list(textos)

        # Avulso inicial da matéria
        for t in textos:
            desc = (t.get("DescricaoTipoTexto") or "").strip().lower()
            link = t.get("UrlTexto") or t.get("Url") or t.get("Link")
            if "avulso inicial da matéria" == desc and isinstance(link, str) and link.startswith("http"):
                return link
        # PDFs relevantes
        prefer = ("projeto","parecer","substitutivo","emenda","requerimento","texto")
        for t in textos:
            desc = (t.get("DescricaoTipoTexto") or "").lower()
            fmt  = (t.get("FormatoTexto") or t.get("TipoDocumento") or "").lower()
            link = t.get("UrlTexto") or t.get("Url") or t.get("Link")
            if isinstance(link, str) and link.startswith("http") and ("pdf" in fmt or link.lower().endswith(".pdf")):
                if any(k in desc for k in prefer):
                    return link
        # qualquer PDF
        for t in textos:
            fmt  = (t.get("FormatoTexto") or t.get("TipoDocumento") or "").lower()
            link = t.get("UrlTexto") or t.get("Url") or t.get("Link")
            if isinstance(link, str) and link.startswith("http") and ("pdf" in fmt or link.lower().endswith(".pdf")):
                return link
        # primeiro válido
        for t in textos:
            link = t.get("UrlTexto") or t.get("Url") or t.get("Link")
            if isinstance(link, str) and link.startswith("http"):
                return link
    return None

def _senado_inteiro_teor_url_from_page(codigo_materia: int|str):
    page = f"https://www25.senado.leg.br/web/atividade/materias/-/materia/{codigo_materia}"
    r = requests.get(page, timeout=40, headers=HDR)
    if r.status_code != 200: return None
    soup = BeautifulSoup(r.text, "html.parser")

    def pick_first(anchors):
        for a in anchors:
            href = (a.get("href") or "").strip()
            if href.startswith("http") and ("sdleg-getter/documento" in href or href.lower().endswith(".pdf")):
                return href
        return None

    anchors = soup.select("a.sf-texto-materia--link")
    avulso = [a for a in anchors if "avulso inicial da matéria" in (a.get("title") or a.get_text("") or "").lower()]
    return pick_first(avulso) or pick_first(anchors) or pick_first(soup.find_all("a"))

def _senado_inteiro_teor_url(codigo_materia: int|str):
    return _senado_inteiro_teor_url_from_api(codigo_materia) or _senado_inteiro_teor_url_from_page(codigo_materia)

def senado_df_hoje() -> pd.DataFrame:
    params = {"dataInicioApresentacao": today_compact(), "dataFimApresentacao": today_compact()}
    r = requests.get(BASE_PESQUISA_SF, params=params, timeout=60, headers=HDR); r.raise_for_status()
    j = r.json()
    materias = (_dig(j, ("PesquisaBasicaMateria","Materias","Materia"))
                or _dig(j, ("PesquisaBasicaMateria","Materia"))
                or _dig(j, ("Materias","Materia"))
                or j.get("Materia") or [])
    materias = _as_list(materias)

    rows = []
    for m in materias:
        if not isinstance(m, dict): continue
        dados = m.get("DadosBasicosMateria") if isinstance(m.get("DadosBasicosMateria"), dict) else {}
        ident = m.get("IdentificacaoMateria") if isinstance(m.get("IdentificacaoMateria"), dict) else {}
        codigo = (m.get("Codigo") or (ident or {}).get("CodigoMateria"))
        sigla  = (m.get("Sigla") or (dados or {}).get("SiglaSubtipoMateria") or (dados or {}).get("SiglaMateria")
                  or (ident or {}).get("SiglaSubtipoMateria") or (ident or {}).get("SiglaMateria"))
        numero = (m.get("Numero") or (dados or {}).get("NumeroMateria") or (ident or {}).get("NumeroMateria"))
        ano    = (m.get("Ano")    or (dados or {}).get("AnoMateria")    or (ident or {}).get("AnoMateria"))
        data   = (m.get("Data")   or (dados or {}).get("DataApresentacao") or m.get("DataApresentacao"))
        ementa = (m.get("Ementa") or (dados or {}).get("EmentaMateria") or m.get("EmentaMateria"))

        inteiro = _senado_inteiro_teor_url(codigo)
        rows.append({
            "uid": f"Senado:{codigo}",
            "casa":"Senado","codigo":codigo,"sigla":sigla,"numero":numero,"ano":ano,
            "data_apresentacao": pd.to_datetime(data, errors="coerce").strftime("%Y-%m-%d") if data else "",
            "ementa":ementa,
            "linkPagina": f"https://www25.senado.leg.br/web/atividade/materias/-/materia/{codigo}",
            "inteiro_teor_url": inteiro or "",
            "ingest_at": now_br().strftime("%Y-%m-%d %H:%M:%S"),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["data_apresentacao","codigo"], ascending=[False, False]).reset_index(drop=True)
    return df

# ====================== CÂMARA ======================
BASE_CAMARA = "https://dadosabertos.camara.leg.br/api/v2/proposicoes"

def _parse_data_apresentacao_camara_text(v: str | None) -> str | None:
    if not v: return None
    v = str(v).strip()
    if len(v) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", v):
        return v[:10]
    try:
        ts = pd.to_datetime(v, errors="raise")
        return ts.strftime("%Y-%m-%d")
    except Exception:
        return None

def _camara_inteiro_teor_url(prop_id:int):
    # A) urlInteiroTeor
    try:
        r = requests.get(f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{prop_id}",
                         timeout=30, headers=HDR)
        if r.status_code == 200:
            dados = r.json().get("dados", {})
            u = dados.get("urlInteiroTeor")
            if isinstance(u, str) and u.startswith("http"):
                return u
    except Exception:
        pass
    # B) /inteiroTeor
    try:
        r = requests.get(f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{prop_id}/inteiroTeor",
                         timeout=30, headers=HDR)
        if r.status_code == 200:
            for d in _as_list(r.json().get("dados", [])):
                u = d.get("url") or d.get("uri") or d.get("link")
                if isinstance(u, str) and u.startswith("http"): return u
    except Exception:
        pass
    # C) /documentos
    try:
        r = requests.get(f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{prop_id}/documentos",
                         timeout=30, headers=HDR)
        if r.status_code == 200:
            docs = _as_list(r.json().get("dados", []))
            for d in docs:
                desc = (d.get("tipoDescricao") or d.get("titulo") or "").lower()
                u = d.get("url") or d.get("uri") or d.get("link")
                if isinstance(u, str) and u.startswith("http"):
                    if "inteiro" in desc or "teor" in desc or u.lower().endswith(".pdf"):
                        return u
            for d in docs:
                u = d.get("url") or d.get("uri") or d.get("link")
                if isinstance(u, str) and u.startswith("http"): return u
    except Exception:
        pass
    return None

def camara_df_hoje() -> pd.DataFrame:
    params = {"dataApresentacaoInicio": today_iso(),
              "dataApresentacaoFim": today_iso(),
              "ordem":"DESC","ordenarPor":"id","itens":100,"pagina":1}
    rows = []
    while True:
        r = requests.get(BASE_CAMARA, params=params, timeout=60, headers=HDR); r.raise_for_status()
        j = r.json()
        for d in j.get("dados", []):
            pid = d.get("id")
            data = _parse_data_apresentacao_camara_text(d.get("dataApresentacao"))
            if data is None:
                # fallback: detalhe
                try:
                    r2 = requests.get(f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{pid}",
                                      timeout=20, headers=HDR)
                    if r2.status_code == 200:
                        det = r2.json().get("dados", {})
                        data = (_parse_data_apresentacao_camara_text(det.get("dataApresentacao"))
                                or _parse_data_apresentacao_camara_text((det.get("statusProposicao") or {}).get("dataHora")))
                except Exception:
                    pass
            rows.append({
                "uid": f"Camara:{pid}",
                "casa":"Camara","id":pid,"sigla":d.get("siglaTipo"),
                "numero":d.get("numero"),"ano":d.get("ano"),
                "data_apresentacao": data or "",
                "ementa":d.get("ementa"),
                "linkPagina":f"https://www.camara.leg.br/propostas-legislativas/{pid}",
                "inteiro_teor_url": _camara_inteiro_teor_url(pid) or "",
                "ingest_at": now_br().strftime("%Y-%m-%d %H:%M:%S"),
            })
        next_link = next((lk for lk in j.get("links", []) if lk.get("rel")=="next"), None)
        if not next_link: break
        params["pagina"] += 1
        time.sleep(0.15)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["data_apresentacao","id"], ascending=[False, False]).reset_index(drop=True)
    return df

# ====================== APPEND no Google Sheets (dedupe) ======================
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")  # defina no Actions/locais
SHEET_SENADO = os.environ.get("SHEET_SENADO", "Senado")
SHEET_CAMARA = os.environ.get("SHEET_CAMARA", "Camara")
CREDENTIALS_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")

def append_dedupe(df: pd.DataFrame, sheet_name: str):
    """
    Lê UIDs existentes, filtra apenas novos e faz append em lote.
    Se a aba não existir, cria com header.
    """
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread_dataframe import set_with_dataframe

    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDENTIALS_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    # cria worksheet se não existir
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows="100", cols="20")
        # escreve header
        set_with_dataframe(ws, df.head(0))
    
    # lê UIDs existentes (coluna A)
    existing = set()
    try:
        colA = ws.col_values(1)  # 1-based
        existing = set(colA[1:])  # ignora header
    except Exception:
        pass

    new_df = df[~df["uid"].astype(str).isin(existing)].copy()
    if new_df.empty:
        print(f"[{sheet_name}] nada novo para anexar.")
        return

    # garante tudo como str (Sheets-friendly)
    for c in new_df.columns:
        new_df[c] = new_df[c].astype(str)

    # append mantendo header apenas se a planilha estiver vazia
    values = [new_df.columns.tolist()] + new_df.values.tolist() if len(existing)==0 else new_df.values.tolist()
    ws.append_rows(values, value_input_option="RAW")
    print(f"[{sheet_name}] adicionadas {len(new_df)} linhas novas.")

def main():
    senado = senado_df_hoje()
    camara = camara_df_hoje()

    print("Senado:", len(senado), "linhas | Câmara:", len(camara), "linhas")

    if SPREADSHEET_ID:
        append_dedupe(senado, SHEET_SENADO)
        append_dedupe(camara, SHEET_CAMARA)
    else:
        # saída local (debug)
        stamp = today_compact()
        senado.to_csv(f"senado_todas_{stamp}.csv", index=False)
        camara.to_csv(f"camara_todas_{stamp}.csv", index=False)
        print("Arquivos locais salvos.")

if __name__ == "__main__":
    main()
