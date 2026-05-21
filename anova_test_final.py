import pandas as pd
import scipy.stats as stats
import json
import html
from datetime import datetime, timezone
from pathlib import Path


project_root = Path(__file__).resolve().parent
comp_rel_path = Path("data") / "core" / "dataset_comparado.csv"
maes_rel_path = Path("data") / "processed" / "dataset_maestro_epidemiologico.csv"


alpha = 0.05

def format_p_value(p):
    if p < 0.0001:
        return f"{p:.4e}"
    else:
        return f"{p:.4f}"

def build_anova_report(title, group1_name, group1_data, group2_name, group2_data):
    lines = []
    lines.append("============================================================")
    lines.append(f"ANÁLISIS ANOVA - {title}")
    lines.append("")
    lines.append("Hipótesis:")
    lines.append("H0: Las medias de ambos datasets son iguales.")
    lines.append("H1: Las medias de ambos datasets son diferentes.")

    mean1 = group1_data.mean()
    n1 = len(group1_data)
    lines.append("")
    lines.append(f"{group1_name}:")
    lines.append(f"Media = {mean1:.4f}")
    lines.append(f"N = {n1}")

    mean2 = group2_data.mean()
    n2 = len(group2_data)
    lines.append("")
    lines.append(f"{group2_name}:")
    lines.append(f"Media = {mean2:.4f}")
    lines.append(f"N = {n2}")

    lines.append("")
    try:
        f_stat, p_val = stats.f_oneway(group1_data, group2_data)
        lines.append("Resultados del ANOVA:")
        lines.append(f"Estadístico F = {f_stat:.4f}")
        lines.append(f"p-valor = {format_p_value(p_val)}")
        lines.append(f"alpha = {alpha}")

        lines.append("")
        lines.append("Decisión:")
        if p_val < alpha:
            lines.append("Se rechaza H0.")
            lines.append("")
            lines.append("Interpretación:")
            lines.append("Existe evidencia estadísticamente significativa para afirmar")
            lines.append("que los promedios de ambos datasets son diferentes.")
        else:
            lines.append("No se rechaza H0.")
            lines.append("")
            lines.append("Interpretación:")
            lines.append("No existe evidencia estadísticamente significativa para afirmar")
            lines.append("que los promedios de ambos datasets sean diferentes.")
    except Exception as e:
        lines.append("Resultados del ANOVA:")
        lines.append(f"Error al ejecutar ANOVA: {type(e).__name__}: {e}")

    lines.append("============================================================")
    lines.append("")
    return "\n".join(lines)


def save_report_to_ipynb(report_parts, output_path):
    cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "# Resultados del test ANOVA\n",
                "\n",
                f"- Generado: {datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}\n",
                f"- alpha: {alpha}\n",
                f"- dataset_comparado: {comp_rel_path.as_posix()}\n",
                f"- dataset_maestro: {maes_rel_path.as_posix()}\n",
            ],
        }
    ]

    for part in report_parts:
        if part == "CONCLUSIÓN GENERAL:":
            cells.append(
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["## CONCLUSIÓN GENERAL\n"],
                }
            )
            continue

        if part.strip().startswith("============================================================"):
            cells.append(
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "<pre>\n",
                        html.escape(part),
                        "\n</pre>\n",
                    ],
                }
            )
            continue

        cells.append(
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [line for line in part.splitlines(keepends=True)],
            }
        )

    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(notebook, ensure_ascii=False, indent=2), encoding="utf-8")


def generate_report_parts(project_root_path=None):
    base = Path(project_root_path) if project_root_path is not None else project_root
    comp_path = base / comp_rel_path
    maes_path = base / maes_rel_path

    df_comp = pd.read_csv(comp_path)
    df_maes = pd.read_csv(maes_path)
    df_comp_dengue = df_comp[df_comp["disease"].astype(str).str.upper() == "DENGUE"]

    grupo_comp_casos = df_comp_dengue["cases_total"].dropna()
    grupo_maes_casos = df_maes["casos_totales"].dropna()

    report_parts = []
    report_parts.append(
        build_anova_report(
            "CASOS TOTALES",
            "Dataset Comparado",
            grupo_comp_casos,
            "Dataset Maestro",
            grupo_maes_casos,
        )
    )

    grupo_comp_temp = df_comp_dengue[df_comp_dengue["temp_avg_c"] > 0.0]["temp_avg_c"].dropna()
    grupo_maes_temp = df_maes["temperatura_promedio"].dropna()

    report_parts.append(
        build_anova_report(
            "TEMPERATURA PROMEDIO (°C)",
            "Dataset Comparado (Excluyendo ceros imputados)",
            grupo_comp_temp,
            "Dataset Maestro",
            grupo_maes_temp,
        )
    )

    grupo_comp_precip = df_comp_dengue["precipitation_mm"].dropna()
    grupo_maes_precip = df_maes["precipitacion_promedio"].dropna()

    report_parts.append(
        build_anova_report(
            "PRECIPITACIÓN PROMEDIO (mm)",
            "Dataset Comparado",
            grupo_comp_precip,
            "Dataset Maestro",
            grupo_maes_precip,
        )
    )

    report_parts.append("CONCLUSIÓN GENERAL:")
    report_parts.append(
        "\n".join(
            [
                """
Aunque ambos datasets pertenecen al mismo contexto de estudio,
presentan diferencias importantes en los valores promedio de precipitación.

Dataset comparado: 133.16 mm
Dataset maestro: 102.58 mm

Esto puede indicar varias posibilidades:

- diferencias en la fuente de recolección de datos
- distintos periodos de tiempo analizados
- diferencias en la cobertura geográfica
- procesos distintos de limpieza o transformación de datos
- presencia de datos faltantes o atípicos
- metodologías diferentes de medición
""",
            ]
        )
    )

    return report_parts


def generate_report_text(project_root_path=None):
    return "\n".join(generate_report_parts(project_root_path=project_root_path))


def main():
    report_parts = generate_report_parts()
    final_report = "\n".join(report_parts)
    print(final_report)

    ipynb_output_path = Path(__file__).with_name("anova_test_final_resultados.ipynb")
    save_report_to_ipynb(report_parts, ipynb_output_path)
    print(f"Notebook guardado en: {ipynb_output_path}")


if __name__ == "__main__":
    main()
