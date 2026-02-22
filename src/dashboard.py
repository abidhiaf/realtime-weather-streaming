import streamlit as st
import pandas as pd
import os
import plotly.express as px

st.set_page_config(page_title="Weather Dashboard", layout="wide")

st.title("🌦️ Weather Streaming Dashboard by ABIDHIAF")

# ================= LOAD =================

folder = "data/weather_lake"
files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".parquet")]

df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
df["event_time"] = pd.to_datetime(df["event_time"])

# cacher temp_diff
df_display = df.drop(columns=["temp_diff"])

# ================= FILTRES FULL WIDTH =================

st.subheader("🎛️ Filtres interactifs")

fcol1, fcol2 = st.columns([3,1])

with fcol1:
    cities = st.multiselect(
        "🌍 Choisir les villes",
        df.city.unique(),
        default=df.city.unique(),
        placeholder="Sélectionner une ou plusieurs villes..."
    )

with fcol2:
    date_range = st.date_input(
        "📅 Période",
        [df.event_time.min(), df.event_time.max()]
    )

df_filtered = df[
    (df.city.isin(cities)) &
    (df.event_time.dt.date >= date_range[0]) &
    (df.event_time.dt.date <= date_range[1])
]

st.divider()

# ================= KPI =================

col1, col2, col3, col4 = st.columns(4)

col1.metric("🌡️ Temp moyenne", round(df_filtered.temperature.mean(),1))
col2.metric("💧 Humidité moyenne", round(df_filtered.humidity.mean(),1))
col3.metric("🔥 Ville la plus chaude",
            df_filtered.groupby("city").temperature.mean().idxmax())
col4.metric("📊 Observations", len(df_filtered))

st.divider()

# ================= GRAPH 1 =================

st.subheader("🌡️ Température moyenne par ville")

avg_city = df_filtered.groupby("city").temperature.mean().reset_index()

fig1 = px.bar(avg_city, x="city", y="temperature", color="temperature")
st.plotly_chart(fig1, use_container_width=True)

# ================= GRAPH 2 =================

st.subheader("📈 Evolution température")

fig2 = px.line(df_filtered, x="event_time", y="temperature", color="city")
st.plotly_chart(fig2, use_container_width=True)

# ================= GRAPH 3 (PIE HUMIDITE) =================


st.subheader("💧 Humidité moyenne par ville")

hum_city = df_filtered.groupby("city").humidity.mean().reset_index()

fig3 = px.bar(hum_city, x="city", y="humidity")
st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ================= TOP ANALYTICS =================

st.header("⭐ Analyse avancée des villes")

stats = df_filtered.groupby("city").agg({
    "temperature":"mean",
    "humidity":"mean"
}).reset_index()

# 🔥 TOP CHAUDES
top_hot = stats.sort_values("temperature", ascending=False).head(5)

fig_hot = px.bar(
    top_hot,
    x="temperature",
    y="city",
    orientation="h",
    title="🔥 Top 5 villes les plus chaudes",
    color="temperature",
    color_continuous_scale="reds"
)

st.plotly_chart(fig_hot, use_container_width=True)

# ❄️ TOP FROIDES
top_cold = stats.sort_values("temperature", ascending=True).head(5)

fig_cold = px.bar(
    top_cold,
    x="temperature",
    y="city",
    orientation="h",
    title="❄️ Top 5 villes les plus froides",
    color="temperature",
    color_continuous_scale="blues"
)

st.plotly_chart(fig_cold, use_container_width=True)

# 💧 TOP HUMIDES
top_hum = stats.sort_values("humidity", ascending=False).head(5)

fig_hum = px.bar(
    top_hum,
    x="humidity",
    y="city",
    orientation="h",
    title="💧 Top 5 villes les plus humides",
    color="humidity",
    color_continuous_scale="teal"
)

st.plotly_chart(fig_hum, use_container_width=True)

st.divider()

# ================= TABLE =================

st.subheader("📋 Données récentes")

st.dataframe(
    df_display.sort_values("event_time", ascending=False).head(50),
    use_container_width=True
)