# LLM Proposal — Agente Conversacional Fintrust

## Propósito

Agente conversacional especializado en el modelo analítico de Fintrust.
Permite a usuarios de negocio consultar en lenguaje natural qué métricas y
tablas están disponibles en la capa analytics, sin necesidad de conocer SQL
ni la estructura técnica del modelo de datos.

---

## Alcance del agente

El agente **únicamente** responde preguntas sobre:

- Descripción de las tablas y vistas del modelo `analytics`
- Métricas disponibles en cada tabla o vista

Cualquier pregunta fuera de ese alcance recibe como respuesta:
> *"No tengo información al respecto"*

---

## Tablas y vistas que conoce

| Nombre | Tipo | Pregunta que responde |
|---|---|---|
| `dm_cartera` | Data Mart | Fuente base de cartera, mora y recaudo por cuota |
| `vw_daily_snapshot` | Vista | KPIs ejecutivos del día por producto, segmento y ciudad |
| `vw_desembolsos_dia_ciudad_segmento` | Vista | ¿Cuánto desembolso se originó por día y ciudad? |
| `vw_saldo_segmento` | Vista | ¿Cuál es el saldo vigente y vencido por segmento? |
| `vw_recaudo_mora` | Vista | ¿Qué % del recaudo del día cubrió cuotas en mora? |
| `vw_cohort_deterioro` | Vista | ¿Qué cohortes muestran mayor deterioro temprano? |
| `vw_top10_creditos_atraso` | Vista | Top 10 créditos con mayor atraso y saldo pendiente |

---

## Stack técnico

| Componente | Tecnología |
|---|---|
| Interfaz | Streamlit |
| LLM | Google Gemini (free tier) — `gemini-2.5-flash` |
| SDK | `google-generativeai` |
| Conocimiento | `knowledge.txt` (system prompt externo, editable sin tocar código) |

---

## Archivos

```
bonus/LLMs/
  agente.py        Aplicación Streamlit — lógica del agente
  knowledge.txt    System prompt con descripción de tablas y métricas
  requirements.txt Dependencias: streamlit, google-generativeai
  llm_proposal.md  Este documento
```

---

## Ejecución

Desde la raíz del proyecto `IDE-001-fintrust`:

```bash
# 1. Instalar dependencias del agente
pip install -r bonus/LLMs/requirements.txt

# 2. Lanzar la interfaz
streamlit run bonus/LLMs/agente.py
```

Streamlit abrirá automáticamente el navegador en `http://localhost:8501`.

---

## Comportamiento ante errores

- Si el modelo configurado no está disponible para la API key, el agente
  detecta automáticamente el primer modelo alternativo válido y lo muestra
  en el sidebar.
- Si se agota la cuota del free tier, reintenta automáticamente hasta 3 veces
  con espera progresiva (45s, 90s, 135s) antes de informar al usuario.

---

## Actualizar el conocimiento del agente

Para agregar una nueva tabla o cambiar la descripción de una vista, editar
únicamente `knowledge.txt`. No es necesario modificar `agente.py`.
