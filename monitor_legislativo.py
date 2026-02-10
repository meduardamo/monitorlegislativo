import os, re, time, requests, pandas as pd, unicodedata
from datetime import datetime
from urllib.parse import urlparse

# Timezone BR
try:
    from zoneinfo import ZoneInfo
except Exception:  # py<3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore

TZ_BR = ZoneInfo("America/Sao_Paulo")
now_br = lambda: datetime.now(TZ_BR)
today_iso = lambda: now_br().date().strftime("%Y-%m-%d")
today_compact = lambda: now_br().date().strftime("%Y%m%d")

# HTTP
HDR = {
    "Accept": "application/json,text/html,*/*",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126 Safari/537.36"
}

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
_sess = requests.Session()
_retry = Retry(total=3, backoff_factor=0.3, status_forcelist=(500, 502, 503, 504))
_sess.headers.update(HDR)
_sess.mount("https://", HTTPAdapter(max_retries=_retry))
_sess.mount("http://",  HTTPAdapter(max_retries=_retry))

# suprimir warning se cair no fallback verify=False
try:
    from urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)  # type: ignore
except Exception:
    pass

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

def _normalize(text: str) -> str:
    if text is None: return ""
    t = unicodedata.normalize("NFD", str(text))
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return t.lower().strip()

def _normalize_ws(s: str) -> str:
    s = _normalize(s)
    return re.sub(r'[^a-z0-9]+', ' ', s).strip()

def _join_unique(seq):
    return ", ".join(dict.fromkeys([x for x in _as_list(seq) if x]))

def _dedup_preserve(seq):
    return list(dict.fromkeys([x for x in _as_list(seq) if x]))

def _label_with_party_uf(nome: str|None, partido: str|None=None, uf: str|None=None) -> str:
    nome = (nome or "").strip()
    p = (partido or "").strip() if partido else ""
    u = (uf or "").strip() if uf else ""
    if not nome:
        return ""
    if p and u:
        return f"{nome} ({p}/{u})"
    if p:
        return f"{nome} ({p})"
    if u:
        return f"{nome} ({u})"
    return nome

# ---------------------- GET helpers ----------------------
def _get_default(url, **kw):
    return _sess.get(url, **kw)

def _get_senado(url, **kw):
    """
    GET p/ Senado com fallback de SSL:
    - tenta com verificação normal
    - se der SSLError, repete com verify=False (se SENADO_INSECURE_FALLBACK != '0')
    """
    try:
        return _sess.get(url, **kw)
    except requests.exceptions.SSLError:
        if os.getenv("SENADO_INSECURE_FALLBACK", "1") != "1":
            raise
        kw2 = dict(kw); kw2["verify"] = False
        return _sess.get(url, **kw2)

# Mapa: Cliente → Tema → Keywords (whole-word)
CLIENT_THEME_DATA = """
IAS|Educação|matemática; alfabetização; alfabetização matemática; recomposição de aprendizagem; plano nacional de educação;
ISG|Educação|tempo integral; fundeb; ensino técnico profissionalizante; educação profissional e tecnológica; ensino médio; propag; infraestrutura escolar; ensino fundamental integral; alfabetização integral; escola em tempo integral; programa escola em tempo integral; ensino fundamental em tempo integral
IU|Educação|recomposição da aprendizagem; educação em tempo integral; fundeb; educação e equidade; educação profissional e tecnológica; ensino técnico profissionalizante
Reúna|Educação|matemática; alfabetização; alfabetização matemática; recomposição de aprendizagem; plano nacional de educação; emendas parlamentares
REMS|Esportes|esporte e desenvolvimento social; esporte e educação; esporte e equidade; paradesporto; desenvolvimento social; esporte educacional
FMCSV|Primeira infância|criança; criança feliz; alfabetização; creche; conanda; maternidade; parentalidade; paternidade; primeira infância; infantil; infância; fundeb; educação básica; plano nacional de educação; pne; homeschooling
IEPS|Saúde|sus; sistema único de saúde; equidade em saúde; atenção primária à saúde; vigilância epidemiológica; planos de saúde; caps; seguros de saúde; populações vulneráveis; desigualdades sociais; organização do sus; políticas públicas em saúde; governança do sus; regionalização em saúde; população negra em saúde; saúde indígena; povos originários; saúde da pessoa idosa; envelhecimento ativo; atenção primária; saúde da criança; saúde do adolescente; saúde da mulher; saúde do homem; saúde da pessoa com deficiência; saúde da população lgbtqia+; financiamento da saúde; emendas e orçamento da saúde; emendas parlamentares; ministério da saúde; trabalhadores e profissionais de saúde; força de trabalho em saúde; política de recursos humanos em saúde; formação profissional de saúde; cuidados primários em saúde; emergências climáticas e ambientais em saúde; emergências climáticas; mudanças ambientais; adaptação climática; saúde ambiental; políticas climáticas; vigilância em saúde; epidemiológica; emergência em saúde; estado de emergência; saúde suplementar; seguradoras; planos populares; anvisa; ans; sandbox regulatório; cartões e administradoras de benefícios em saúde; economia solidária em saúde mental; pessoa em situação de rua; saúde mental; fiscalização de comunidades terapêuticas; rede de atenção psicossocial; raps; unidades de acolhimento; assistência multiprofissional; centros de convivência; cannabis; canabidiol; tratamento terapêutico; desinstitucionalização; manicômios; hospitais de custódia; saúde mental na infância; adolescência; escolas; comunidades escolares; protagonismo juvenil; dependência química; vícios; ludopatia; treinamento; capacitação em saúde mental; intervenções terapêuticas em saúde mental; internet e redes sociais na saúde mental; violência psicológica; surto psicótico
Manual|Saúde|ozempic; wegovy; mounjaro; telemedicina; telessaúde; cbd; cannabis medicinal; cfm; conselho federal de medicina; farmácia magistral; medicamentos manipulados; minoxidil; emagrecedores; retenção de receita; tirzepatida; liraglutida
Mevo|Saúde|prontuário eletrônico; dispensação eletrônica; telessaúde; assinatura digital; certificado digital; controle sanitário; prescrição por enfermeiros; doenças crônicas; responsabilização de plataformas digitais; regulamentação de marketplaces; segurança cibernética; inteligência artificial; digitalização do sus; venda e distribuição de medicamentos; bula digital; atesta cfm; sistemas de controle de farmácia; sngpc; farmacêutico remoto; medicamentos isentos de prescrição; rede nacional de dados em saúde; interoperabilidade; listas de substâncias entorpecentes, psicotrópicas, precursoras e outras; substâncias entorpecentes; substâncias psicotrópicas; substâncias precursoras; substâncias sob controle especial; tabela sus; saúde digital; seidigi; icp-brasil; farmácia popular; cmed
Umane|Saúde|sus; sistema único de saúde; atenção primária à saúde; aps; vigilância epidemiológica; planos de saúde; caps; equidade em saúde; populações vulneráveis; desigualdades sociais; organização do sus; políticas públicas em saúde; governança do sus; regionalização em saúde; população negra em saúde; saúde indígena; povos originários; saúde da pessoa idosa; envelhecimento ativo; atenção primária; saúde da criança; saúde do adolescente; saúde da mulher; saúde do homem; saúde da pessoa com deficiência; saúde da população lgbtqia+; financiamento da saúde; emendas e orçamento da saúde; emendas parlamentares; ministério da saúde; trabalhadores e profissionais de saúde; força de trabalho em saúde; política de recursos humanos em saúde; formação profissional de saúde; cuidados primários em saúde; emergências climáticas e ambientais em saúde; emergências climáticas; mudanças ambientais; adaptação climática; saúde ambiental; políticas climáticas; vigilância em saúde; epidemiológica; emergência em saúde; estado de emergência; saúde suplementar; seguradoras; planos populares; anvisa; ans; sandbox regulatório; cartões e administradoras de benefícios em saúde; conass; conasems
Cactus|Saúde|saúde mental; saúde mental para meninas; saúde mental para juventude; saúde mental para mulheres; pse; eca; rede de atenção psicossocial; raps; caps; centro de apoio psicossocial; programa saúde na escola; bullying; cyberbullying; eca digital
Vital Strategies|Saúde|saúde mental; dados para a saúde; morte evitável; doenças crônicas não transmissíveis; rotulagem de bebidas alcoólicas; educação em saúde; bebidas alcoólicas; imposto seletivo; dcnts; rotulagem de alimentos; alimentos ultraprocessados; publicidade infantil; publicidade de alimentos ultraprocessados; tributação de bebidas alcoólicas; alíquota de bebidas alcoólicas; cigarro eletrônico; controle de tabaco; violência doméstica; exposição a fatores de risco; departamento de saúde mental; hipertensão arterial; saúde digital; violência contra crianças; violência contra mulheres; feminicídio; cop 30
Coletivo Feminista|Direitos reprodutivos|aborto; nascituro; gestação acima de 22 semanas; interrupção legal da gestação; interrupção da gestação; resolução 258 conanda; vida por nascer; vida desde a concepção; criança por nascer; infanticídio; feticídio; assistolia fetal; medicamento abortivo; misoprostol; citotec; cytotec; mifepristona; ventre; assassinato de bebês; luto parental; síndrome pós aborto
IDEC|Saúde|defesa do consumidor; ação civil pública; sac; reforma tributária; ultraprocessados; doenças crônicas não transmissíveis; dcnts; obesidade; codex alimentarius; gordura trans; adoçantes; edulcorantes; rotulagem de alimentos; transgênicos; organismos geneticamente modificados; ogms; marketing e publicidade de alimentos; comunicação mercadológica; escolas e alimentação escolar; bebidas açucaradas; refrigerante; programa nacional de alimentação escolar; pnae; educação alimentar e nutricional; ean; agrotóxicos; pesticidas; defensivos fitossanitários; tributação de alimentos não saudáveis; desertos alimentares; desperdício de alimentos; segurança alimentar e nutricional; direito humano à alimentação; fome; sustentabilidade; mudança climática; plástico; gestão de resíduos; economia circular; desmatamento; greenwashing; energia elétrica; encargos tarifários; subsídios na tarifa de energia; descontos na tarifa de energia; energia pré-paga; abertura do mercado de energia para consumidor cativo; mercado livre de energia; qualidade do serviço de energia; serviço de energia; tarifa social de energia elétrica; geração térmica; combustíveis fósseis; transição energética; descarbonização da matriz elétrica; descarbonização; gases de efeito estufa; acordo de paris; objetivos do desenvolvimento sustentável; reestruturação do setor de energia; reforma do setor elétrico; modernização do setor elétrico; itens de custo da tarifa de energia elétrica; universalização do acesso à energia; eficiência energética; geração distribuída; carvão mineral; painel solar; crédito imobiliário; crédito consignado; publicidade de crédito; cartão de crédito; pagamento de fatura; parcelamento com e sem juros; cartões pré-pagos; programas de fidelidade; cheque especial; taxa de juros; contrato de crédito; endividamento de jovens; crédito estudantil; endividamento de idosos; crédito por meio de aplicativos; abertura e movimentação de conta bancária; cobrança de serviços sem autorização; cadastro positivo; contratação de serviços bancários com imposição de seguros e títulos de capitalização; acessibilidade aos canais de serviços bancários; serviços bancários; caixa eletrônico; internet banking; aplicativos móveis; contratação de pacotes de contas bancárias; acesso à informação em caso de negativa de crédito; plano de saúde; saúde suplementar; medicamentos isentos de prescrição; mip; medicamentos antibióticos; antimicrobianos; propriedade intelectual; patentes; licença compulsória; preços de medicamentos; complexo econômico-industrial da saúde; saúde digital; prontuário eletrônico; rede nacional de dados em saúde; rnds; datasus; proteção de dados pessoais; telessaúde; telecomunicações; internet; tv por assinatura; serviço de acesso condicionado; telefonia móvel; telefonia fixa; tv digital; lei geral de proteção de dados; autoridade nacional de proteção de dados; reconhecimento facial; lei geral de telecomunicações; bens reversíveis; fundo de universalização dos serviços de telecomunicações; provedores de acesso; franquia de internet; marco civil da internet; neutralidade de rede; zero rating; privacidade; lei de acesso à informação; regulação de plataformas digitais; desinformação; fake news; dados biométricos; vazamento de dados; telemarketing; serviço de valor adicionado
""".strip()

def _kw_tokens(kw: str) -> list[str]:
    return [t for t in _normalize_ws(kw).split() if t]

def _compile_kw_pattern(kw: str):
    toks = _kw_tokens(kw)
    if not toks:
        return None
    patt = r'\b' + r'\s+'.join(map(re.escape, toks)) + r'\b'
    return re.compile(patt)

def _parse_client_theme_data(text: str):
    client_theme: dict[str, dict[str, list[str]]] = {}
    for raw in text.splitlines():
        if not raw.strip():
            continue
        cliente, tema, kws = [x.strip() for x in raw.split("|", 2)]
        kw_list = [k.strip() for k in kws.split(";") if k.strip()]
        client_theme.setdefault(cliente, {}).setdefault(tema, [])
        seen = set()
        for k in kw_list:
            kkey = _normalize_ws(k)
            if kkey and kkey not in seen:
                seen.add(kkey)
                client_theme[cliente][tema].append(k)
    return client_theme

CLIENT_THEME = _parse_client_theme_data(CLIENT_THEME_DATA)

# Pré-compila padrões para whole-word
KW_PATTERNS: list[tuple[re.Pattern, str, str, str]] = []
for cliente, temas in CLIENT_THEME.items():
    for tema, kws in temas.items():
        for kw in kws:
            pat = _compile_kw_pattern(kw)
            if pat:
                KW_PATTERNS.append((pat, cliente, tema, kw))

def _extract_kw_client_theme(texto: str):
    nt = _normalize_ws(texto or "")
    matched_kws = []
    pairs = set()
    for pat, cliente, tema, original_kw in KW_PATTERNS:
        if pat.search(nt):
            matched_kws.append(original_kw)
            pairs.add((cliente, tema))
    kw_str = "; ".join(dict.fromkeys(matched_kws).keys())
    clientes_str = "; ".join(sorted({c for c, _ in pairs}))
    temas_str = "; ".join(sorted({t for _, t in pairs}))
    return kw_str, clientes_str, temas_str

# Helpers de DATA/HORA
def _fmt_date(v) -> str:
    try:
        d = pd.to_datetime(v, errors="coerce")
        if pd.isna(d): return ""
        return d.strftime("%Y-%m-%d")
    except Exception:
        return ""

def _fmt_dt(v) -> str:
    try:
        d = pd.to_datetime(v, errors="coerce")
        if pd.isna(d): return ""
        d = d.tz_localize(None) if getattr(d, "tzinfo", None) is not None else d
        return d.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return v.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ""

# Heurística do tipo de autor
_rx_orgao = re.compile(r'(?i)\b(comissao|comissão|mesa|presid[eê]ncia|c[âa]mara dos deputados|senado federal|congresso|comit[eê]|subcomissao|subcomissão)\b')
_rx_exec  = re.compile(r'(?i)\b(poder executivo|presid[eê]ncia da rep[úu]blica|minist[eé]rio|ministro|casa civil)\b')

def _infer_tipo_autor(nome: str|None) -> str:
    n = (nome or "").strip()
    if not n: return ""
    if _rx_orgao.search(n): return "Órgão/Comissão"
    if _rx_exec.search(n):  return "Executivo"
    return "Parlamentar"

#                       SENADO
from bs4 import BeautifulSoup
BASE_PESQUISA_SF = "https://legis.senado.leg.br/dadosabertos/materia/pesquisa/lista.json"

def _senado_textos_api(codigo_materia):
    tries = [
        f"https://legis.senado.leg.br/dadosabertos/materia/textos/{codigo_materia}.json",
        f"https://legis.senado.leg.br/dadosabertos/materia/{codigo_materia}/textos.json",
        f"https://legis.senado.leg.br/dadosabertos/materia/{codigo_materia}.json",
    ]
    for u in tries:
        try:
            r = _get_senado(u, timeout=30)
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
    page = f"https://www25.senado.leg.br/web/atividade/materias/-/materia/{codigo_materia}"
    try:
        r = _get_senado(page, timeout=40)
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

# Regex "Nome (PARTIDO/UF)" com vários autores separados por ';' ou por '), '
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

def _senado_primeira_autoria_da_pagina(codigo_materia) -> str | None:
    url = f"https://www25.senado.leg.br/web/atividade/materias/-/materia/{codigo_materia}"
    try:
        r = _get_senado(url, timeout=45)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        holders = soup.select("div.span12.sf-bloco-paragrafos-condensados") or soup.select("div.bg-info-conteudo") or [soup]
        for holder in holders:
            for p in holder.find_all("p"):
                strong = p.find("strong")
                if not strong: continue
                label = _normalize(strong.get_text(" ", strip=True).rstrip(":"))
                if label == "autoria":
                    span = p.find("span")
                    if span:
                        val = span.get_text(" ", strip=True)
                        if val: return val
                    full = p.get_text(" ", strip=True)
                    val = re.sub(r'(?i)^\s*autoria\s*:\s*', "", full).strip()
                    if val: return val
        return None
    except Exception:
        return None

def senado_df_hoje() -> pd.DataFrame:
    params = {"dataInicioApresentacao": today_compact(), "dataFimApresentacao": today_compact()}
    r = _get_senado(BASE_PESQUISA_SF, params=params, timeout=60); r.raise_for_status()
    j = r.json()
    materias = (_dig(j, ("PesquisaBasicaMateria","Materias","Materia"))
                or _dig(j, ("PesquisaBasicaMateria","Materia"))
                or _dig(j, ("Materias","Materia"))
                or j.get("Materia") or [])
    materias = _as_list(materias)

    rows = []
    for m in materias:
        if not isinstance(m, dict): 
            continue
        dados = m.get("DadosBasicosMateria", {}) if isinstance(m.get("DadosBasicosMateria"), dict) else {}
        ident = m.get("IdentificacaoMateria", {}) if isinstance(m.get("IdentificacaoMateria"), dict) else {}

        codigo = _get(m, "Codigo") or _get(ident, "CodigoMateria")
        sigla  = (_get(m, "Sigla") or _get(dados, "SiglaSubtipoMateria", "SiglaMateria")
                  or _get(ident, "SiglaSubtipoMateria", "SiglaMateria"))
        numero = _get(m, "Numero") or _get(dados, "NumeroMateria") or _get(ident, "NumeroMateria")
        ano    = _get(m, "Ano")    or _get(dados, "AnoMateria")    or _get(ident, "AnoMateria")
        data   = _get(m, "Data")   or _get(dados, "DataApresentacao") or _get(m, "DataApresentacao")
        ementa = (_get(m, "Ementa") or _get(dados, "EmentaMateria") or _get(m, "EmentaMateria") or "")

        # ---- autores (API + fallback texto) ----
        autor_str = _get(m, "Autor")
        nomes, partidos, ufs = [], [], []
        for bloco in ("Autoria","Autores"):
            b = m.get(bloco)
            if isinstance(b, dict):
                alist = b.get("Autor")
                alist = alist if isinstance(alist, list) else [alist]
                for a in alist or []:
                    if not isinstance(a, dict): 
                        continue
                    nome = a.get("NomeAutor") or a.get("NomeParlamentar")
                    partido = (a.get("SiglaPartidoAutor") or a.get("SiglaPartido")
                               or a.get("PartidoAutor") or a.get("Partido"))
                    uf = a.get("UfAutor") or a.get("SiglaUF") or a.get("UF")
                    if nome: nomes.append(nome)
                    partidos.append(partido if partido else None)
                    ufs.append(uf if uf else None)

        if _normalize(autor_str) == _normalize("Câmara dos Deputados"):
            autor_page = _senado_primeira_autoria_da_pagina(codigo)
            if autor_page:
                autor_str = autor_page

        if autor_str:
            n2, p2, u2 = _parse_autores_senado_texto(autor_str)
            if n2 and not nomes: nomes = n2
            if any(p2) and not any(partidos): partidos = p2
            if any(u2) and not any(ufs): ufs = u2

        # granular + coautores "Nome (PARTIDO/UF)"
        if nomes:
            ap_nome = nomes[0]
            ap_part = partidos[0] if len(partidos) else None
            ap_uf   = ufs[0] if len(ufs) else None
            co_list = []
            for i in range(1, len(nomes)):
                p = partidos[i] if i < len(partidos) else None
                u = ufs[i] if i < len(ufs) else None
                co_list.append(_label_with_party_uf(nomes[i], p, u))
            co_list = _dedup_preserve([x for x in co_list if x])
            coau = ", ".join(co_list)
            qtd_coaut = len(co_list)
        else:
            ap_nome = autor_str or ""
            ap_part = None
            ap_uf   = None
            coau    = ""
            qtd_coaut = 0
        ap_tipo = _infer_tipo_autor(ap_nome)

        it_url, _ = _senado_inteiro_teor(codigo)
        kw_str, clientes_str, temas_str = _extract_kw_client_theme(ementa)

        rows.append({
            "UID": f"Senado:{codigo}",
            "Casa Atual": "Senado",
            "Sigla": sigla, "Número": numero, "Ano": ano,
            "Data Apresentação": _fmt_date(data),
            "Ementa": ementa,
            "Palavras Chave": kw_str,
            "Clientes": clientes_str,
            "Temas": temas_str,
            # autoria granular
            "Autor Principal": ap_nome,
            "Autor Principal Partido": ap_part or "",
            "Autor Principal UF": ap_uf or "",
            "Autor Principal Tipo": ap_tipo,
            "Coautores": coau,
            "Qtd Coautores": str(qtd_coaut),
            # links / auditoria
            "Link Página": f"https://www25.senado.leg.br/web/atividade/materias/-/materia/{codigo}",
            "Inteiro Teor URL": it_url or "",
            "Ingest At": _fmt_dt(now_br()),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["Data Apresentação","UID"], ascending=[False, False]).reset_index(drop=True)
    return df

#                       CÂMARA
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
        r = _get_default(f"{BASE_DEP}/{dep_id}", timeout=25)
        r.raise_for_status()
        dados = r.json().get("dados", {})
        status = dados.get("ultimoStatus", {}) if isinstance(dados.get("ultimoStatus"), dict) else {}
        partido = status.get("siglaPartido") or dados.get("siglaPartido")
        uf = status.get("siglaUf") or dados.get("uf")
        return partido, uf
    except Exception:
        return (None, None)

def _autores_camara_completo(prop_id:int) -> dict:
    out = []
    url = f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{prop_id}/autores"
    try:
        r = _get_default(url, timeout=30); r.raise_for_status()
        for a in r.json().get("dados", []):
            nome = a.get("nome") or ""
            uri  = a.get("uri")
            tipo = (a.get("tipo") or a.get("tipoAutor") or a.get("tipoAssinatura") or "").strip()
            ordem = a.get("ordemAssinatura") or a.get("ordem")
            dep_id = _last_int_from_uri(uri) if uri and "/deputados/" in uri else None
            partido, uf = _get_deputado_partido_uf(dep_id) if dep_id else (None, None)
            out.append({
                "nome": nome,
                "partido": partido or "",
                "uf": uf or "",
                "tipo": tipo,
                "ordem": int(ordem) if isinstance(ordem, int) or (isinstance(ordem, str) and ordem.isdigit()) else None,
                "is_dep": bool(dep_id),
            })
    except Exception:
        pass

    # escolher principal
    ap = None
    for a in out:
        if a.get("ordem") == 1:
            ap = a; break
    if not ap:
        for a in out:
            if re.search(r'(?i)\bautor\b', a.get("tipo","")) and not re.search(r'(?i)\bcoautor\b', a.get("tipo","")):
                ap = a; break
    if not ap:
        for a in out:
            if a.get("is_dep"):
                ap = a; break
    if not ap and out:
        ap = out[0]

    autor_principal = ap["nome"] if ap else ""
    autor_principal_part = ap["partido"] if ap else ""
    autor_principal_uf = ap["uf"] if ap else ""
    autor_principal_tipo = "Parlamentar" if (ap and ap.get("is_dep")) else (_infer_tipo_autor(autor_principal) if autor_principal else "")

    # coautores como "Nome (PARTIDO/UF)"
    co_labels = _dedup_preserve([
        _label_with_party_uf(a["nome"], a.get("partido"), a.get("uf"))
        for a in out if a is not ap and a.get("nome")
    ])
    coautores = ", ".join(co_labels)
    qtd_coaut = len(co_labels)

    return {
        "ap_nome": autor_principal,
        "ap_partido": autor_principal_part,
        "ap_uf": autor_principal_uf,
        "ap_tipo": autor_principal_tipo,
        "coautores": coautores,
        "qtd_coaut": str(qtd_coaut),
    }

def _camara_inteiro_teor(prop_id:int):
    try:
        r = _get_default(f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{prop_id}", timeout=30)
        if r.status_code == 200:
            dados = r.json().get("dados", {})
            u = dados.get("urlInteiroTeor")
            if isinstance(u, str) and u.startswith("http"):
                return u, ""
    except Exception:
        pass
    try:
        r = _get_default(f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{prop_id}/inteiroTeor", timeout=30)
        if r.status_code == 200:
            for d in _as_list(r.json().get("dados", [])):
                u = d.get("url") or d.get("uri") or d.get("link")
                dt = d.get("dataHora") or d.get("data")
                if isinstance(u, str) and u.startswith("http"):
                    return u, (str(dt)[:19] if dt else "")
    except Exception:
        pass
    try:
        r = _get_default(f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{prop_id}/documentos", timeout=30)
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
        r = _get_default(BASE_CAMARA, params=params, timeout=60); r.raise_for_status()
        j = r.json()
        for d in j.get("dados", []):
            pid = d.get("id")
            data = _parse_data_apresentacao_camara_text(d.get("dataApresentacao"))
            if data is None:
                try:
                    r2 = _get_default(f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{pid}", timeout=20)
                    if r2.status_code == 200:
                        det = r2.json().get("dados", {})
                        data = (_parse_data_apresentacao_camara_text(det.get("dataApresentacao"))
                                or _parse_data_apresentacao_camara_text((det.get("statusProposicao") or {}).get("dataHora")))
                except Exception:
                    pass

            autores = _autores_camara_completo(pid)
            it_url, _ = _camara_inteiro_teor(pid)
            ementa = d.get("ementa", "") or ""
            kw_str, clientes_str, temas_str = _extract_kw_client_theme(ementa)

            rows.append({
                "UID": f"Camara:{pid}",
                "Casa Atual": "Camara",
                "Sigla": d.get("siglaTipo"),
                "Número": d.get("numero"),
                "Ano": d.get("ano"),
                "Data Apresentação": _fmt_date(data),
                "Ementa": ementa,
                "Palavras Chave": kw_str,
                "Clientes": clientes_str,
                "Temas": temas_str,
                # autoria granular
                "Autor Principal": autores.get("ap_nome",""),
                "Autor Principal Partido": autores.get("ap_partido",""),
                "Autor Principal UF": autores.get("ap_uf",""),
                "Autor Principal Tipo": autores.get("ap_tipo",""),
                "Coautores": autores.get("coautores",""),
                "Qtd Coautores": autores.get("qtd_coaut","0"),
                # links / auditoria
                "Link Página": f"https://www.camara.leg.br/propostas-legislativas/{pid}",
                "Inteiro Teor URL": it_url or "",
                "Ingest At": _fmt_dt(now_br()),
            })
        next_link = next((lk for lk in j.get("links", []) if lk.get("rel")=="next"), None)
        if not next_link: break
        params["pagina"] += 1
        time.sleep(0.15)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["Data Apresentação","UID"], ascending=[False, False]).reset_index(drop=True)
    return df

#                 INSERÇÃO no Google Sheets (dedupe, topo)
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")  # planilha geral
SHEET_SENADO   = os.environ.get("SHEET_SENADO", "Senado")
SHEET_CAMARA   = os.environ.get("SHEET_CAMARA", "Camara")

# nova planilha com abas por cliente (Câmara + Senado)
SPREADSHEET_ID_CLIENTES = os.environ.get("SPREADSHEET_ID_CLIENTES")

CREDENTIALS_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")

NEEDED_COLUMNS = [
    "UID","Casa Atual",
    "Sigla","Número","Ano",
    "Data Apresentação","Ementa",
    "Palavras Chave","Clientes","Temas",
    # autoria granular
    "Autor Principal","Autor Principal Partido","Autor Principal UF","Autor Principal Tipo",
    "Coautores","Qtd Coautores",
    # links e auditoria
    "Link Página","Inteiro Teor URL",
    "Ingest At",
]

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in NEEDED_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    for c in NEEDED_COLUMNS:
        df[c] = df[c].fillna("").astype(str)
    return df[NEEDED_COLUMNS].copy()

def _open_sheet(spreadsheet_id: str):
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDENTIALS_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(spreadsheet_id)

def ensure_headers(spreadsheet_id: str, sheet_names: list[str]):
    """NO-OP: não cria abas e não altera cabeçalhos. Apenas checa se existem."""
    if not spreadsheet_id or not sheet_names:
        return
    sh = _open_sheet(spreadsheet_id)
    for name in sheet_names:
        try:
            sh.worksheet(name)
        except Exception:
            print(f"[{name}] aba não encontrada na planilha {spreadsheet_id} — pulando (não crio automaticamente).")

# Helpers de alinhamento/insert
def _sheet_header(ws) -> list[str]:
    try:
        hdr = ws.row_values(1)
        return [h.strip() for h in hdr] if hdr else []
    except Exception:
        return []

def _align_df_to_ws_header(df: pd.DataFrame, ws) -> pd.DataFrame:
    """Reordena/completa df conforme o cabeçalho REAL da aba."""
    header = _sheet_header(ws)
    if not header:
        header = NEEDED_COLUMNS[:]  # fallback passivo (sem escrever nada no sheet)
    out = df.copy()
    for h in header:
        if h not in out.columns:
            out[h] = ""
    out = out[header]
    out = out.fillna("").astype(str)
    return out

def _existing_uids(ws) -> set[str]:
    """UIDs existentes (coluna A, da linha 2 em diante)."""
    try:
        rng = ws.batch_get(['A2:A'], value_render_option='UNFORMATTED_VALUE')
        col = rng[0] if rng and rng[0] else []
        return set(v[0] for v in col if v and v[0])
    except Exception:
        vals = ws.col_values(1)[1:]
        return set(v for v in vals if v)

def _insert_rows_top(ws, rows: list[list], chunk_size: int = 500):
    """Insere as linhas na posição 2 preservando a ordem fornecida."""
    idx = 0
    while idx < len(rows):
        ws.insert_rows(rows[idx:idx+chunk_size], row=2, value_input_option="USER_ENTERED")
        idx += chunk_size

def insert_dedupe_top(df: pd.DataFrame, sheet_name: str):
    """Insere (não append) somente linhas novas (por UID) no TOPO (linha 2)."""
    if df is None or df.empty:
        print(f"[{sheet_name}] nenhum dado para enviar.")
        return
    if not SPREADSHEET_ID:
        print("SPREADSHEET_ID não definido; pulando envio ao Sheets.")
        return

    sh = _open_sheet(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(sheet_name)
    except Exception:
        print(f"[{sheet_name}] aba inexistente na planilha geral — pulando (não crio automaticamente).")
        return

    df = _normalize_columns(df)
    exists = _existing_uids(ws)
    new_df = df[~df["UID"].isin(exists)].copy()

    if new_df.empty:
        print(f"[{sheet_name}] nada novo para inserir.")
        return

    new_df = new_df.sort_values(["Data Apresentação","UID"], ascending=[False, False]).reset_index(drop=True)
    aligned = _align_df_to_ws_header(new_df, ws)
    rows = aligned.values.tolist()
    _insert_rows_top(ws, rows)
    print(f"[{sheet_name}] inseridas {len(rows)} linhas novas no topo.")

def insert_por_cliente_top(df_total: pd.DataFrame):
    """Envio p/ SPREADSHEET_ID_CLIENTES, uma aba por cliente, inserindo no topo (linha 2)."""
    if not SPREADSHEET_ID_CLIENTES:
        print("SPREADSHEET_ID_CLIENTES não definido; pulando planilha por cliente.")
        return

    if df_total is None or df_total.empty:
        print("[clientes] nada a enviar.")
        return

    sh = _open_sheet(SPREADSHEET_ID_CLIENTES)
    df_total = _normalize_columns(df_total)

    all_clients = list(CLIENT_THEME.keys())
    for client in all_clients:
        mask = df_total["Clientes"].str.contains(rf'(^|;\s*){re.escape(client)}(\s*;|$)', case=False, na=False)
        sub = df_total[mask].copy()
        sheet_name = client

        if sub.empty:
            print(f"[{sheet_name}] sem linhas novas hoje.")
            continue

        try:
            ws = sh.worksheet(sheet_name)
        except Exception:
            print(f"[{sheet_name}] aba inexistente na planilha de clientes — pulando (não crio automaticamente).")
            continue

        exists = _existing_uids(ws)
        new_df = sub[~sub["UID"].isin(exists)].copy()
        if new_df.empty:
            print(f"[{sheet_name}] nada novo para inserir.")
            continue

        new_df = new_df.sort_values(["Data Apresentação","UID"], ascending=[False, False]).reset_index(drop=True)
        aligned = _align_df_to_ws_header(new_df, ws)
        rows = aligned.values.tolist()
        _insert_rows_top(ws, rows)
        print(f"[{sheet_name}] inseridas {len(rows)} linhas novas no topo.")

#                        MAIN
def main():
    senado = senado_df_hoje()
    camara = camara_df_hoje()

    print(f"Senado: {len(senado)} linhas | Câmara: {len(camara)} linhas")

    # Checa existência das abas (não cria / não altera cabeçalho)
    if SPREADSHEET_ID:
        ensure_headers(SPREADSHEET_ID, [SHEET_SENADO, SHEET_CAMARA])
    if SPREADSHEET_ID_CLIENTES:
        ensure_headers(SPREADSHEET_ID_CLIENTES, list(CLIENT_THEME.keys()))

    if not SPREADSHEET_ID and not SPREADSHEET_ID_CLIENTES:
        stamp = today_compact()
        senado.to_csv(f"senado_{stamp}.csv", index=False)
        camara.to_csv(f"camara_{stamp}.csv", index=False)
        print("Sem IDs de planilha; arquivos CSV salvos.")
        return

    # 1) Planilha geral — INSERÇÃO NO TOPO
    if SPREADSHEET_ID:
        insert_dedupe_top(senado, SHEET_SENADO)
        insert_dedupe_top(camara, SHEET_CAMARA)

    # 2) Planilha por cliente (Câmara + Senado combinados) — INSERÇÃO NO TOPO
    if SPREADSHEET_ID_CLIENTES:
        if senado is None or senado.empty:
            total = camara.copy()
        elif camara is None or camara.empty:
            total = senado.copy()
        else:
            total = pd.concat([senado, camara], ignore_index=True)
        insert_por_cliente_top(total)

if __name__ == "__main__":
    main()
