# Proyecto II: Análisis de la Calidad del Agua en Ríos de la India

## Procesamiento de Datos con PySpark MLlib & Keras sobre Cluster con Apache Spark y Hadoop HDFS

**Autor:** Jonatan Alejandro Gallo Martinez  
**Universidad:** Pontificia Universidad Javeriana — Bogotá  
**Curso:** Computación de Alto Desempeño  
**Fecha:** 28 de mayo de 2026

---

## Video de Presentación

[![Ver Video](https://img.shields.io/badge/YouTube-Ver%20Presentación-red?style=for-the-badge&logo=youtube)](https://youtu.be/se1WUQAYY1A)

> **Enlace:** https://youtu.be/se1WUQAYY1A

---

## Contexto

El monitoreo de la calidad del agua en los ríos de la India requiere analizar grandes volúmenes de datos fisicoquímicos y microbiológicos provenientes de **534 estaciones** distribuidas en **18 estados**. Este proyecto aplica técnicas de Procesamiento de Alto Volumen de Datos (PAVD) para diagnosticar la calidad hídrica mediante modelos de Machine Learning ejecutados sobre un clúster HPC con Apache Spark.

**Infraestructura utilizada:**
- Clúster HPC de la Pontificia Universidad Javeriana
- Apache Spark 3.5.5, 12 workers, 240 núcleos, 756 GiB RAM
- Datos almacenados en HDFS (`hdfs://10.195.34.34:9000/csv/waterquality.csv`)
- TensorFlow 2.21.0 para el modelo de Red Neuronal

---

## Objetivo

Implementar y comparar múltiples modelos de aprendizaje automático (Keras/TensorFlow, Random Forest, Gradient Boosting y Regresión Lineal) para predecir el **Índice de Calidad del Agua (WQI)** en ríos de la India, evaluando su rendimiento mediante métricas estándar y seleccionando el modelo más adecuado.

---

## Metodología

### Parámetros del WQI

| Parámetro | Peso | Descripción |
|---|---|---|
| pH | 0.165 | Acidez/alcalinidad del agua |
| DO (Oxígeno Disuelto) | 0.281 | Concentraciones altas = mejor calidad |
| Conductividad | 0.234 | Capacidad de conducción eléctrica |
| BOD | 0.009 | Demanda Bioquímica de Oxígeno |
| Nitratos/Nitritos | 0.028 | Crecimiento de algas |
| Coliformes Fecales | 0.281 | Contaminación microbiológica |

**Fórmula:**
```
WQI = qrPH×0.165 + qrDO×0.281 + qrCOND×0.234 + qrBOD×0.009 + qrNN×0.028 + qrFecal×0.281
```

**Referencia:** [Singh, S., & Pant, K. K. (2021). Water Quality Index. IntechOpen](https://www.intechopen.com/chapters/69568)

---

## Análisis Exploratorio de Datos (EDA)

### Parámetros DO y pH por estación

![DO y pH](Img/Parametro%20DO%20y%20pH%20.png)

El oxígeno disuelto presenta alta variabilidad entre estaciones, con caídas a valores cercanos a cero en puntos críticos. El pH se mantiene estable alrededor de 7.5–8.0, con picos ocasionales que superan 13, indicando contaminación alcalina severa.

### Parámetros BOD y Nitratos/Nitritos

![BOD y Nitratos](Img/BOD%20y%20Nitrogenos.png)

El BOD registra picos que superan 75 mg/L en estaciones con alta carga orgánica. Los nitratos/nitritos se mantienen bajos en la mayoría de estaciones pero presentan picos localizados hasta 45 mg/L.

### Conductividad y Coliformes Fecales

![Conductividad y Coliformes](Img/indicativos%20de%20contaminacion.png)

FECAL_COLIFORM es el parámetro más crítico del dataset: presenta valores extremos que superan los **310,000 UFC/100mL**, evidenciando contaminación microbiológica severa. La conductividad se mantiene relativamente baja y estable en comparación.

---

## Resultados

### Distribución Geoespacial del WQI — Mapa de la India

![Mapa WQI India](Img/Mapa%20Calidad%20Agua.png)

La mayoría del territorio presenta WQI en el rango 50–75 (calidad Baja). Los estados del sur y occidente como Goa, Maharashtra y Andhra Pradesh muestran los índices más altos (calidad Muy Baja). Punjab se destaca con los valores más bajos, indicando mejor calidad relativa del agua.

### WQI por Estado — Histograma

![Histograma por Estado](Img/Histograma%20de%20la%20calidad.png)

Punjab presenta el WQI más bajo (~38–43), siendo el estado con mejor calidad relativa. Rajastán y Madhya Pradesh superan 90, clasificando en Muy Baja. Ningún estado alcanza WQI < 25 (Excelente) a nivel promedio.

### Distribución general de calidad

| Categoría | Estaciones | Porcentaje |
|---|---|---|
| Baja (WQI 50-75) | 269 | 50.4% |
| Buena (WQI 25-50) | 166 | 31.1% |
| Muy Baja (WQI 75-100) | 71 | 13.3% |
| Excelente (WQI 0-25) | 28 | 5.2% |
| **Total** | **534** | **100%** |

> El **63.7%** de las estaciones presenta calidad **Baja o Muy Baja**, indicando deterioro hídrico generalizado.

### Estadísticas del WQI

| Estadístico | Valor |
|---|---|
| Media | 57.957 |
| Desviación estándar | 15.987 |
| Mínimo | 10.120 |
| Mediana | 59.180 |
| Máximo | 95.120 |

---

## Evaluación y Comparación de Modelos

### WQI Real vs WQI Predicho — Todos los modelos

![WQI Real vs Predicho](Img/WQI%20Real%20vs%20WQI%20Predicho.png)

Keras y Regresión Lineal prácticamente se superponen con la diagonal perfecta. GBT muestra mayor dispersión en los extremos que la Regresión Lineal, y Random Forest es el modelo con mayor desviación.

### Tabla de métricas — partición 80/20, seed=42

| Modelo | MSE | RMSE | MAE | R² |
|---|---|---|---|---|
| **Regresión Lineal MLlib** | 0.0000 | 0.0000 | 0.0000 | **1.0000** |
| **Red Neuronal Keras** | 0.0300 | 0.1732 | 0.0519 | **0.9999** |
| GBT MLlib (adicional) | 11.5887 | 3.4042 | 0.9444 | 0.9608 |
| Random Forest MLlib | 15.7384 | 3.9672 | 2.5402 | 0.9467 |

**La Regresión Lineal obtiene R² = 1.0** porque el WQI es por definición una combinación lineal exacta de los features utilizados. Esto valida la correcta implementación del pipeline de PySpark.

### Importancia de Variables

| Feature | Random Forest | GBT |
|---|---|---|
| qrFecal (Coliformes) | **0.4448** | **0.4922** |
| qrDO (Oxígeno Disuelto) | 0.2962 | 0.3150 |
| qrCOND (Conductividad) | 0.1527 | 0.1454 |
| qrBOD | 0.0785 | 0.0114 |
| qrPH | 0.0262 | 0.0355 |
| qrNN (Nitratos) | 0.0016 | 0.0005 |

> **Coliformes fecales** concentra el **44–49%** de la importancia en ambos modelos de ensamble.

---

## Hallazgos Clave

1. **63.7% de las estaciones tienen agua de calidad deficiente** — solo el 5.2% alcanza calidad Excelente.
2. **Coliformes fecales es el parámetro más crítico** del WQI (44–49% de importancia).
3. **La Regresión Lineal alcanza R² perfecto** — confirma la relación matemática exacta entre WQI y sus componentes.
4. **GBT supera a Random Forest** en modelos de ensamble (RMSE 3.40 vs 3.97).
5. **Keras ejecutó 200 épocas** con loss de entrenamiento 0.0005 y validación 0.0228, logrando R² = 0.9999.
6. **PySpark MLlib permite entrenamiento distribuido** sobre el clúster de 12 workers sin dependencia de sklearn.

---

## Recomendaciones

1. Usar parámetros fisicoquímicos originales (valores continuos) como features para explotar relaciones no lineales reales.
2. Priorizar intervenciones en saneamiento de coliformes fecales por su impacto dominante en el WQI.
3. Escalar el dataset con series históricas mensuales para justificar el uso completo del clúster HDFS.
4. Encapsular el flujo en un MLlib Pipeline serializable para despliegue en producción.
5. Persistir los modelos entrenados en HDFS para reutilización sin reentrenamiento.

---

## Estructura del Repositorio

```
├── Clean_ML_Water_Gallo.ipynb       # Notebook principal con todo el análisis)
├── Presentacion_Calidad_Agua_Gallo.pdf    # Presentación en formato PDF
├── waterquality.csv                 # Dataset (534 registros)
├── Indian_States.shp                # Shapefile de estados de la India
├── Indian_States.dbf
├── Indian_States.prj
├── Indian_States.shx
├── Img/
│   ├── BOD y Nitrogenos.png
│   ├── Histograma de la calidad.png
│   ├── indicativos de contaminacion.png
│   ├── Mapa Calidad Agua.png
│   ├── Parametro DO y pH .png
│   └── WQI Real vs WQI Predicho.png
└── README.md
```

## Tecnologías Utilizadas

- **Apache Spark 3.5.5** — Motor de procesamiento distribuido
- **PySpark MLlib** — RandomForestRegressor, GBTRegressor, LinearRegression, VectorAssembler, RegressionEvaluator
- **TensorFlow/Keras 2.21.0** — Red Neuronal Sequential (3 capas ocultas, 350 neuronas)
- **Hadoop HDFS** — Almacenamiento distribuido de datos
- **GeoPandas** — Visualización geoespacial sobre shapefile de la India
- **Matplotlib / Seaborn** — Gráficas de parámetros y resultados
- **Python 3 / NumPy / Pandas** — Procesamiento numérico y tabular

## Cómo Ejecutar

1. Conectarse al clúster HPC de la Javeriana (nodo maestro).
2. Verificar que el archivo `waterquality.csv` esté cargado en HDFS:
   ```bash
   /mnt/sda1/Cluster/Hadoop/bin/hadoop fs -ls /csv
   ```
3. Abrir Jupyter Notebook y ejecutar `Clean_ML_Water_Gallo.ipynb` celda por celda.
4. El notebook genera automáticamente todas las gráficas, mapas y métricas de los modelos.

## Referencias

1. Singh, S., & Pant, K. K. (2021). *Water Quality Index*. IntechOpen. https://www.intechopen.com/chapters/69568
2. Apache Spark. (2024). *MLlib: Machine Learning Library Guide — Spark 3.5.5*. https://spark.apache.org/docs/3.5.5/ml-guide.html
3. TensorFlow. (2023, June 8). Keras: The high-level API for TensorFlow. https://www.tensorflow.org/guide/keras
4. Breiman, L. (2001). Random Forests. *Machine Learning*, 45(1), 5-32. https://doi.org/10.1023/A:1010933404324
5. Friedman, J. H. (2001). Greedy Function Approximation: A Gradient Boosting Machine. *Annals of Statistics*, 29(5), 1189-1232.
6. Maharashtra Pollution Control Board. (2021). Annexure I: Water quality criteria (CPCB). https://mpcb.ecmpcb.in/images/pdf/WaterQuality0709/AnnexureI_WQ.pdf
7. GeoPandas Developers. (2024). *GeoPandas Documentation 0.14*. https://geopandas.org/

---

**Pontificia Universidad Javeriana — Proyecto II: Análisis de la Calidad del Agua — Mayo 2026**
