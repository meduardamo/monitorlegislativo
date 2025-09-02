# monitor_legislativo.py
import os, re, time, requests, pandas as pd
from datetime import datetime
from urllib.parse import urlparse

# ====================== Timezone BR ======================
try:
    from zoneinfo import ZoneInfo
except Exception:  # py<3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore

TZ_BR = ZoneInfo("America/Sao_Paulo")
now_br = lambda: datetime.now(TZ_BR)
today_iso = lambda: now_br().date().strftime("%Y-%m-%d")
today_compact = lambda: now_br().date().strftime("%Y%m%d")

# ====================== HTTP ======================
HDR = {
    "Accept": "application/json,text/html,*/*",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126 Safari/537.36"
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

def _get(d: dict, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d[k]
    return default

def _last_int_from_uri(u: str|None):
    if not u: return None
    try:
        return int(urlparse(u).path.rstrip("/").split("/")[-1])
    except Exception:
        return None

# =========================================================
#                       SENADO
# =========================================================
from bs4 import BeautifulSoup
BASE_PESQUISA_SF = "https://legis.senado.leg.br/dadosabertos/materia/pesquisa/lista.json"

def _senado_textos_api(codigo_materia):
    """Retorna lista de dicts de textos da matéria (ou [])."""
    tries = [
        f"https://legis.senado.leg.br/dadosabertos/materia/textos/{codigo_materia}.json",
        f"https://legis.senado.leg.br/dadosabertos/materia/{codigo_materia}/textos.json",
        f"https://legis.senado.leg.br/dadosabertos/materia/{codigo_materia}.json",
    ]
    for u in tries:
        try:
            r = requests.get(u, timeout=30, headers=HDR)
            if r.status_code != 200:
                continue
            j = r.json()
            textos = (_dig(j, ("TextoMateria","Textos","Texto"))
                      or _dig(j, ("Textos","Texto"))
                      or j.get("Textos") or [])
            return _as_list(textos)
        except Exception:
            pass
    return []

def _senado_inteiro_teor_api(codigo_materia):
    """
    Tenta escolher o link do inteiro teor nos 'Textos' da API.
    Retorna (url, dataTexto) ou (None, None).
    Preferência:
      1) 'Avulso inicial da matéria'
      2) PDFs com descrições mais prováveis
      3) qualquer PDF
      4) primeiro link http
    """
    textos = _senado_textos_api(codigo_materia)
    if not textos:
        return None, None

    def extract(td):
        desc = (td.get("DescricaoTipoTexto") or "").strip()
        form = (td.get("FormatoTexto") or td.get("TipoDocumento") or "").lower()
        url  = td.get("UrlTexto") or td.get("Url") or td.get("Link")
        data = td.get("DataTexto") or td.get("Data")
        return desc, form, url, data

    for t in textos:
        desc, form, url, data = extract(t)
        if desc.lower() == "avulso inicial da matéria" and isinstance(url, str) and url.startswith("http"):
            return url, (str(data) if data else None)

    prefer = ("projeto","parecer","substitutivo","emenda","requerimento","texto")

    for t in textos:
        desc, form, url, data = extract(t)
        if isinstance(url, str) and url.startswith("http"):
            if ("pdf" in form or url.lower().endswith(".pdf")) and any(k in desc.lower() for k in prefer):
                return url, (str(data) if data else None)

    for t in textos:
        desc, form, url, data = extract(t)
        if isinstance(url, str) and url.startswith("http") and ("pdf" in form or url.lower().endswith(".pdf")):
            return url, (str(data) if data else None)

    for t in textos:
        desc, form, url, data = extract(t)
        if isinstance(url, str) and url.startswith("http"):
            return url, (str(data) if data else None)

    return None, None

def _senado_inteiro_teor_page(codigo_materia):
    """Fallback pela página pública. Retorna (url, None)."""
    page = f"https://www25.senado.leg.br/web/atividade/materias/-/materia/{codigo_materia}"
    try:
        r = requests.get(page, timeout=40, headers=HDR)
        if r.status_code != 200:
            return None, None
        soup = BeautifulSoup(r.text, "html.parser")

        def pick_first(anchors):
            for a in anchors:
                href = (a.get("href") or "").strip()
                if href.startswith("http") and ("sdleg-getter/documento" in href or href.lower().endswith(".pdf")):
                    return href
            return None

        anchors = soup.select("a.sf-texto-materia--link")
        avulso = [a for a in anchors if "avulso inicial da matéria" in (a.get("title") or a.get_text("") or "").lower()]
        u = pick_first(avulso) or pick_first(anchors) or pick_first(soup.find_all("a"))
        return (u, None) if u else (None, None)
    except Exception:
        return None, None

def _senado_inteiro_teor(codigo_materia):
    u, d = _senado_inteiro_teor_api(codigo_materia)
    if u: return u, d
    return _senado_inteiro_teor_page(codigo_materia)

_rx_autor_chunk = re.compile(r"""\s*
    (?P<nome>.+?)
    (?:\s*\(\s*(?P<partido>[A-ZÀ-Ü\-]+)\s*/\s*(?P<uf>[A-Z]{2})\s*\))?
    \s*$""", re.X)

def _parse_autores_senado_texto(autor_str: str):
    if not autor_str:
        return [], [], []
    chunks = [c.strip() for c in re.split(r";", autor_str) if c and c.strip()]
    if len(chunks) == 1:
        if '), ' in autor_str:
            parts = [p + (')' if not p.endswith(')') else '') for p in autor_str.split('), ')]
            chunks = [p.strip() for p in parts]
        else:
            chunks = [c.strip() for c in autor_str.split(',') if c.strip()]
    nomes, partidos, ufs = [], [], []
    for ch in chunks:
        m = _rx_autor_chunk.match(ch)
        if m:
            nomes.append(m.group('nome'))
            partidos.append(m.group('partido'))
            ufs.append(m.group('uf'))
        else:
            nomes.append(ch); partidos.append(None); ufs.append(None)
    return nomes, partidos, ufs

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
        dados = m.get("DadosBasicosMateria", {}) if isinstance(m.get("DadosBasicosMateria"), dict) else {}
        ident = m.get("IdentificacaoMateria", {}) if isinstance(m.get("IdentificacaoMateria"), dict) else {}

        codigo = _get(m, "Codigo") or _get(ident, "CodigoMateria")
        sigla  = (_get(m, "Sigla") or _get(dados, "SiglaSubtipoMateria", "SiglaMateria")
                  or _get(ident, "SiglaSubtipoMateria", "SiglaMateria"))
        numero = _get(m, "Numero") or _get(dados, "NumeroMateria") or _get(ident, "NumeroMateria")
        ano    = _get(m, "Ano")    or _get(dados, "AnoMateria")    or _get(ident, "AnoMateria")
        data   = _get(m, "Data")   or _get(dados, "DataApresentacao") or _get(m, "DataApresentacao")
        ementa = _get(m, "Ementa") or _get(dados, "EmentaMateria") or _get(m, "EmentaMateria")

        autor_str = _get(m, "Autor")
        nomes, partidos, ufs = [], [], []
        for bloco in ("Autoria","Autores"):
            b = m.get(bloco)
            if isinstance(b, dict):
                alist = b.get("Autor")
                alist = alist if isinstance(alist, list) else [alist]
                for a in alist or []:
                    if not isinstance(a, dict): continue
                    nome = a.get("NomeAutor") or a.get("NomeParlamentar")
                    partido = (a.get("SiglaPartidoAutor") or a.get("SiglaPartido") or
                               a.get("PartidoAutor") or a.get("Partido"))
                    uf = a.get("UfAutor") or a.get("SiglaUF") or a.get("UF")
                    if nome: nomes.append(nome)
                    partidos.append(partido if partido else None)
                    ufs.append(uf if uf else None)
        if (not any(partidos) or not any(ufs)) and autor_str:
            n2, p2, u2 = _parse_autores_senado_texto(autor_str)
            if n2 and not nomes: nomes = n2
            if not any(partidos): partidos = p2
            if not any(ufs): ufs = u2
        if not autor_str and nomes:
            autor_str = ", ".join(nomes)

        it_url, _ = _senado_inteiro_teor(codigo)

        rows.append({
            "uid": f"Senado:{codigo}",
            "casa": "Senado",
            "sigla": sigla, "numero": numero, "ano": ano,
            "data_apresentacao": (pd.to_datetime(data, errors="coerce").strftime("%Y-%m-%d") if data else ""),
            "ementa": ementa or "",
            "autor": autor_str or "",
            "autor_partidos": ", ".join([p for p in partidos if p]) if any(partidos) else "",
            "autor_ufs": ", ".join([u for u in ufs if u]) if any(ufs) else "",
            "linkPagina": f"https://www25.senado.leg.br/web/atividade/materias/-/materia/{codigo}",
            "inteiro_teor_url": it_url or "",
            "ingest_at": now_br().strftime("%Y-%m-%d %H:%M:%S"),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["data_apresentacao","uid"], ascending=[False, False]).reset_index(drop=True)
    return df

# =========================================================
#                       CÂMARA
# =========================================================
BASE_CAMARA = "https://dadosabertos.camara.leg.br/api/v2/proposicoes"
BASE_DEP    = "https://dadosabertos.camara.leg.br/api/v2/deputados"

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

def _get_deputado_partido_uf(dep_id: int):
    if not dep_id: return (None, None)
    try:
        r = requests.get(f"{BASE_DEP}/{dep_id}", timeout=25, headers=HDR)
        r.raise_for_status()
        dados = r.json().get("dados", {})
        status = dados.get("ultimoStatus", {}) if isinstance(dados.get("ultimoStatus"), dict) else {}
        partido = status.get("siglaPartido") or dados.get("siglaPartido")
        uf = status.get("siglaUf") or dados.get("uf")
        return partido, uf
    except Exception:
        return (None, None)

def _autores_camara_completo(prop_id:int) -> dict:
    out_nomes, out_partidos, out_ufs = [], [], []
    url = f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{prop_id}/autores"
    try:
        r = requests.get(url, timeout=30, headers=HDR)
        r.raise_for_status()
        for a in r.json().get("dados", []):
            nome = a.get("nome")
            uri  = a.get("uri")
            dep_id = _last_int_from_uri(uri) if uri and "/deputados/" in uri else None
            partido, uf = _get_deputado_partido_uf(dep_id) if dep_id else (None, None)
            if nome: out_nomes.append(nome)
            out_partidos.append(partido)
            out_ufs.append(uf)
    except Exception:
        pass
    return {
        "autor": ", ".join(out_nomes) if out_nomes else "",
        "autor_partidos": ", ".join([p for p in out_partidos if p]) if any(out_partidos) else "",
        "autor_ufs": ", ".join([u for u in out_ufs if u]) if any(out_ufs) else "",
    }

def _camara_inteiro_teor(prop_id:int):
    try:
        r = requests.get(f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{prop_id}",
                         timeout=30, headers=HDR)
        if r.status_code == 200:
            dados = r.json().get("dados", {})
            u = dados.get("urlInteiroTeor")
            if isinstance(u, str) and u.startswith("http"):
                return u, ""
    except Exception:
        pass
    try:
        r = requests.get(f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{prop_id}/inteiroTeor",
                         timeout=30, headers=HDR)
        if r.status_code == 200:
            for d in _as_list(r.json().get("dados", [])):
                u = d.get("url") or d.get("uri") or d.get("link")
                dt = d.get("dataHora") or d.get("data")
                if isinstance(u, str) and u.startswith("http"):
                    return u, (str(dt)[:19] if dt else "")
    except Exception:
        pass
    try:
        r = requests.get(f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{prop_id}/documentos",
                         timeout=30, headers=HDR)
        if r.status_code == 200:
            docs = _as_list(r.json().get("dados", []))
            for d in docs:
                desc = (d.get("tipoDescricao") or d.get("titulo") or "").lower()
                u = d.get("url") or d.get("uri") or d.get("link")
                dt = d.get("dataHora") or d.get("data")
                if isinstance(u, str) and u.startswith("http") and ("inteiro" in desc or "teor" in desc or u.lower().endswith(".pdf")):
                    return u, (str(dt)[:19] if dt else "")
            for d in docs:
                u = d.get("url") or d.get("uri") or d.get("link")
                dt = d.get("dataHora") or d.get("data")
                if isinstance(u, str) and u.startswith("http"):
                    return u, (str(dt)[:19] if dt else "")
    except Exception:
        pass
    return None, None

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
                try:
                    r2 = requests.get(f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{pid}",
                                      timeout=20, headers=HDR)
                    if r2.status_code == 200:
                        det = r2.json().get("dados", {})
                        data = (_parse_data_apresentacao_camara_text(det.get("dataApresentacao"))
                                or _parse_data_apresentacao_camara_text((det.get("statusProposicao") or {}).get("dataHora")))
                except Exception:
                    pass

            autores = _autores_camara_completo(pid)
            it_url, _ = _camara_inteiro_teor(pid)

            rows.append({
                "uid": f"Camara:{pid}",
                "casa": "Camara",
                "sigla": d.get("siglaTipo"),
                "numero": d.get("numero"),
                "ano": d.get("ano"),
                "data_apresentacao": data or "",
                "ementa": d.get("ementa"),
                "autor": autores.get("autor",""),
                "autor_partidos": autores.get("autor_partidos",""),
                "autor_ufs": autores.get("autor_ufs",""),
                "linkPagina": f"https://www.camara.leg.br/propostas-legislativas/{pid}",
                "inteiro_teor_url": it_url or "",
                "ingest_at": now_br().strftime("%Y-%m-%d %H:%M:%S"),
            })
        next_link = next((lk for lk in j.get("links", []) if lk.get("rel")=="next"), None)
        if not next_link: break
        params["pagina"] += 1
        time.sleep(0.15)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["data_apresentacao","uid"], ascending=[False, False]).reset_index(drop=True)
    return df

# =========================================================
#                 APPEND no Google Sheets (dedupe)
# =========================================================
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
SHEET_SENADO   = os.environ.get("SHEET_SENADO", "Senado")
SHEET_CAMARA   = os.environ.get("SHEET_CAMARA", "Camara")
CREDENTIALS_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")

NEEDED_COLUMNS = [
    "uid","casa",
    "sigla","numero","ano",
    "data_apresentacao","ementa",
    "autor","autor_partidos","autor_ufs",
    "linkPagina","inteiro_teor_url",
    "ingest_at",
]

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in NEEDED_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    for c in NEEDED_COLUMNS:
        df[c] = df[c].fillna("").astype(str)
    return df[NEEDED_COLUMNS].copy()

def _ensure_header(ws, header):
    """Se a aba existir mas estiver sem header (ou diferente), escreve o header."""
    first_row = ws.row_values(1)
    if first_row != header:
        ws.resize(rows=max(2, ws.row_count), cols=len(header))
        ws.update('1:1', [header])

def append_dedupe(df: pd.DataFrame, sheet_name: str):
    import gspread
    from google.oauth2.service_account import Credentials

    if df is None or df.empty:
        print(f"[{sheet_name}] nenhum dado para enviar.")
        return
    if not SPREADSHEET_ID:
        print("SPREADSHEET_ID não definido; pulando envio ao Sheets.")
        return

    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDENTIALS_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    df = _normalize_columns(df)

    try:
        ws = sh.worksheet(sheet_name)
        _ensure_header(ws, NEEDED_COLUMNS)
        existing = set(ws.col_values(1)[1:])  # uids existentes (ignora header)
        new_df = df[~df["uid"].isin(existing)].copy()
        if new_df.empty:
            print(f"[{sheet_name}] nada novo para anexar.")
            return
        ws.append_rows(new_df.values.tolist(), value_input_option="RAW")
        print(f"[{sheet_name}] adicionadas {len(new_df)} linhas novas.")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=str(max(100, len(df)+10)), cols=len(NEEDED_COLUMNS))
        _ensure_header(ws, NEEDED_COLUMNS)
        if not df.empty:
            ws.append_rows(df.values.tolist(), value_input_option="RAW")
        print(f"[{sheet_name}] criada e preenchida com {len(df)} linhas.")

# =========================================================
#                        MAIN
# =========================================================
def main():
    senado = senado_df_hoje()
    camara = camara_df_hoje()

    print(f"Senado: {len(senado)} linhas | Câmara: {len(camara)} linhas")

    if not SPREADSHEET_ID:
        # modo local (debug): salva CSVs
        stamp = today_compact()
        senado.to_csv(f"senado_{stamp}.csv", index=False)
        camara.to_csv(f"camara_{stamp}.csv", index=False)
        print("SPREADSHEET_ID não definido; arquivos CSV salvos.")
        return

    append_dedupe(senado, SHEET_SENADO)
    append_dedupe(camara, SHEET_CAMARA)

if __name__ == "__main__":
    main()
