import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
import numpy as np

PATH_CSV = "output"

# Title
st.title("Congestión vehicular en la Carrera 7 en Bogotá, Colombia 2")

# Frequency distribution of speed_average column
st.bar_chart(pd.read_csv(f"{PATH_CSV}/frequency_distribution.csv"),  x="speed_average", y="Frequency")

st.write("Velocidad promedio por año y hora")
data = pd.read_csv(f"{PATH_CSV}/lineplot_speed_average_by_year.csv")[["year", "hour", "speed_average"]]
data["year"] = data["year"].astype(str)
st.table(data.head())
st.line_chart(data, x="hour", y="speed_average", color="year")

# Without outliers
sns.set_style("whitegrid")
fig = plt.figure(figsize=(16, 9))
ax = sns.lineplot(data=pd.read_csv(f"{PATH_CSV}/lineplot_speed_average_by_year.csv"), x="hour", y="speed_average", hue="year")
ax.set_xticks(range(24), labels=["0","1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"])
st.pyplot(fig)
