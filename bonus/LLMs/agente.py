"""
agente.py
Agente conversacional sobre el modelo analítico de Fintrust.
Interfaz: Streamlit  |  LLM: Google Gemini (free tier)

Responde únicamente preguntas sobre:
  - Descripción de tablas/vistas del modelo analytics
  - Métricas disponibles en cada tabla/vista

Cualquier pregunta fuera de ese alcance responde:
  'No tengo información al respecto'

Uso:
    streamlit run agente.py
"""

import time
import warnings
from pathlib import Path
import streamlit as st
warnings.filterwarnings("ignore", category=FutureWarning, module="google")
import google.generativeai as genai

# ---------------------------------------------------------------------------
# API Key y modelo
# ---------------------------------------------------------------------------

GEMINI_API_KEY = ""
GEMINI_MODEL   = "gemini-2.5-flash"

# ---------------------------------------------------------------------------
# Configuración de página
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Fintrust — Asistente de Datos",
    page_icon="📊",
    layout="centered",
)

# ---------------------------------------------------------------------------
# Conocimiento del modelo analytics — leído desde knowledge.txt
# ---------------------------------------------------------------------------

ANALYTICS_KNOWLEDGE = (Path(__file__).parent / "knowledge.txt").read_text(encoding="utf-8")

# ---------------------------------------------------------------------------
# Inicialización del cliente Gemini
# ---------------------------------------------------------------------------

def get_model_name() -> str | None:
    """
    Verifica si GEMINI_MODEL está disponible para la API key.
    Si no, retorna el primer modelo alternativo válido. Si falla, retorna None.
    """
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        available = [
            m.name for m in genai.list_models()
            if "generateContent" in m.supported_generation_methods
        ]
        if not available:
            return None
        match = next((m for m in available if GEMINI_MODEL in m), None)
        if match:
            return match
        # Fallback: preferir gemini-2.0-flash o gemini-1.5-flash si el modelo no existe
        for preferred in ("gemini-2.0-flash", "gemini-1.5-flash"):
            fallback = next((m for m in available if preferred in m), None)
            if fallback:
                return fallback
        return available[0]
    except Exception:
        return None


def init_gemini() -> tuple:
    """Retorna (GenerativeModel, model_name) o (None, None) si falla."""
    model_name = get_model_name()
    if model_name is None:
        return None, None
    model = genai.GenerativeModel(
        model_name=model_name,
        system_instruction=ANALYTICS_KNOWLEDGE,
    )
    return model, model_name


# ---------------------------------------------------------------------------
# Catálogo de tablas/vistas disponibles (refleja 03-analytics/)
# ---------------------------------------------------------------------------

CATALOG = [
    "dm_cartera",
    "vw_daily_snapshot",
    "vw_desembolsos_dia_ciudad_segmento",
    "vw_saldo_segmento",
    "vw_recaudo_mora",
    "vw_cohort_deterioro",
    "vw_top10_creditos_atraso",
]

# ---------------------------------------------------------------------------
# UI principal
# ---------------------------------------------------------------------------

st.title("📊 Fintrust — Asistente de Datos")
st.subheader("Consulta métricas y descripción de las tablas y vistas del modelo analítico.")

# --- Resolver modelo al arrancar (una sola vez por sesión) ---
if "resolved_model_name" not in st.session_state:
    _, resolved = init_gemini()
    st.session_state["resolved_model_name"] = resolved or GEMINI_MODEL

# --- Sidebar ---
with st.sidebar:
    st.header("Modelo activo")
    st.code(st.session_state["resolved_model_name"], language=None)
    if st.session_state["resolved_model_name"] != GEMINI_MODEL:
        st.warning(f"`{GEMINI_MODEL}` no disponible.\nUsando: `{st.session_state['resolved_model_name']}`")

    st.divider()
    st.markdown("**Puedes preguntar sobre estas tablas:**")
    for nombre in CATALOG:
        st.markdown(f"- `{nombre}`")

    st.divider()
    if st.button("Limpiar conversación"):
        st.session_state["messages"] = []
        st.rerun()

# --- Inicializar historial ---
if "messages" not in st.session_state:
    st.session_state["messages"] = []

# --- Mostrar historial ---
for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# --- Prompt: desde chat input O desde botón del sidebar ---
prompt = st.chat_input("Escribe tu pregunta sobre el modelo de datos...")
if not prompt and "pending_prompt" in st.session_state:
    prompt = st.session_state.pop("pending_prompt")

if prompt:

    st.session_state["messages"].append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    model, _ = init_gemini()
    if model is None:
        st.error("No se pudo inicializar el modelo. Revisa la API key en agente.py.")
        st.stop()

    # Construir historial para Gemini (alterna user/model)
    history = []
    for msg in st.session_state["messages"][:-1]:
        role = "user" if msg["role"] == "user" else "model"
        history.append({"role": role, "parts": [msg["content"]]})

    with st.chat_message("assistant"):
        with st.spinner("Consultando..."):
            answer = None
            for intento in range(3):
                try:
                    chat = model.start_chat(history=history)
                    response = chat.send_message(prompt)
                    answer = response.text
                    break
                except Exception as e:
                    msg_error = str(e)
                    # Cuota agotada: esperar el retry_delay indicado por la API
                    if "retry" in msg_error.lower() or "quota" in msg_error.lower():
                        segundos = 45 * (intento + 1)
                        with st.spinner(f"Cuota alcanzada. Reintentando en {segundos}s (intento {intento+1}/3)..."):
                            time.sleep(segundos)
                    else:
                        answer = f"Error al conectar con Gemini: {msg_error}"
                        break
            if answer is None:
                answer = "Se agotó el cupo del modelo por ahora. Intenta de nuevo en unos minutos."

        st.markdown(answer)

    st.session_state["messages"].append({"role": "assistant", "content": answer})
