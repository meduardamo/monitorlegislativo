# monitorlegislativo

Monitoramento de proposições legislativas e organização do output em Google Sheets.
Este repo roda uma coleta principal e depois uma etapa auxiliar de “alinhamento”.

## Arquivos principais
- `monitor_legislativo.py`: rotina principal de monitoramento (entrypoint)
- `alinhamento.py`: rotinas auxiliares (ex.: classificação/alinhamento)
- `.github/workflows/main.yml`: execução automatizada via GitHub Actions
- `requirements.txt`: dependências Python
