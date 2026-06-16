"""
Lecture de sources externes : fichiers Excel & SharePoint (Databricks).

POURQUOI CE MODULE : Spark NE LIT PAS nativement le .xlsx (contrairement au CSV /
Parquet). Et un fichier déposé sur SharePoint n'est pas directement accessible
depuis le cluster. Ce module donne les deux ponts, alignés sur la philosophie du
pipeline (tout lire en STRING puis caster dans clean_* — cf. load_data, fix T-02).

╔══════════════════════════════════════════════════════════════════════════════╗
║ LIRE UN EXCEL DANS DATABRICKS — deux voies                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ 1. pandas + openpyxl  (read_excel_to_spark)                                    ║
║    Simple, driver-side. Idéal < ~1M cellules (un inventaire MRM tient large).  ║
║    Aucune install : openpyxl est dans le Databricks Runtime.                   ║
║                                                                                ║
║ 2. spark-excel  (read_excel_spark_native)                                      ║
║    Lecture DISTRIBUÉE pour les très gros classeurs. Nécessite la lib Maven     ║
║    com.crealytics:spark-excel_2.12:<ver> installée sur le cluster              ║
║    (Cluster ▸ Libraries ▸ Install ▸ Maven). Sinon → voie 1.                    ║
╚══════════════════════════════════════════════════════════════════════════════╝

SHAREPOINT : pas de montage direct fiable. La voie robuste = Microsoft Graph API
(app registration Azure AD + client credentials) : on récupère le fichier en
bytes, on l'écrit sur DBFS, puis on le lit comme un Excel local.
Prérequis (à faire une fois côté Azure/IT) :
    - App registration Azure AD → client_id + client_secret + tenant_id ;
    - permission application Microsoft Graph « Sites.Read.All » (consentement admin) ;
    - STOCKER les secrets dans un Databricks secret scope, JAMAIS en dur :
        dbutils.secrets.get("itip", "sp_client_secret").
"""

import io
import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

GRAPH = "https://graph.microsoft.com/v1.0"


# ============================================================================
# LECTURE EXCEL → SPARK
# ============================================================================

def read_excel_to_spark(
    spark : SparkSession,
    path  : str,
    sheet = 0,
    header: int = 0,
    as_string: bool = True,
) -> DataFrame:
    """
    Lit un .xlsx via pandas+openpyxl et renvoie un DataFrame Spark.

    Args:
        spark     : SparkSession active.
        path      : chemin LOCAL du driver — pour un fichier DBFS, préfixer "/dbfs/"
                    (ex. "dbfs:/FileStore/x.xlsx" → "/dbfs/FileStore/x.xlsx").
        sheet     : nom ou index de l'onglet (0 = premier).
        header    : ligne d'en-tête (0 = première).
        as_string : True = tout lire en STRING (recommandé : les casts ciblés sont
                    faits ensuite dans clean_* — évite les types inférés au hasard,
                    même logique que le CSV MRM en inferSchema=false).

    Returns:
        DataFrame Spark (colonnes nettoyées des espaces de bord).
    """
    import pandas as pd

    pdf = pd.read_excel(
        path, sheet_name=sheet, header=header,
        dtype=str if as_string else None, engine="openpyxl",
    )
    # En-têtes propres + valeurs NaN → None (Spark n'aime pas les float('nan') en string).
    pdf.columns = [str(c).strip() for c in pdf.columns]
    if as_string:
        pdf = pdf.where(pd.notnull(pdf), None)
    logger.info("Excel lu [%s] onglet=%s : %d lignes × %d colonnes",
                path, sheet, len(pdf), len(pdf.columns))
    return spark.createDataFrame(pdf)


def read_excel_spark_native(
    spark : SparkSession,
    path  : str,
    sheet : str = "0",
    header: bool = True,
) -> DataFrame:
    """
    Lecture DISTRIBUÉE d'un Excel via la lib spark-excel (très gros classeurs).

    Nécessite com.crealytics:spark-excel installé sur le cluster. `path` est un
    chemin Spark (ex. "dbfs:/FileStore/x.xlsx"). `sheet` = nom d'onglet ou index.
    inferSchema=false (cohérent avec le reste : casts dans clean_*).
    """
    return (
        spark.read.format("com.crealytics.spark.excel")
        .option("header", str(header).lower())
        .option("inferSchema", "false")
        .option("dataAddress", f"'{sheet}'!A1")   # onglet par nom, ou "0" pour le 1er
        .load(path)
    )


# ============================================================================
# SHAREPOINT (Microsoft Graph)
# ============================================================================

def get_graph_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """
    Jeton d'accès application (client credentials) pour Microsoft Graph.

    À alimenter depuis un secret scope, jamais en dur :
        get_graph_token(tid, dbutils.secrets.get("itip","sp_client_id"), ...)
    """
    import requests

    resp = requests.post(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
            "scope":         "https://graph.microsoft.com/.default",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def download_sharepoint_file(
    token        : str,
    site_hostname: str,
    site_path    : str,
    file_path    : str,
    dest_path    : str,
) -> str:
    """
    Télécharge un fichier SharePoint via Graph et l'écrit sur DBFS.

    Args:
        token         : jeton (get_graph_token).
        site_hostname : ex. "moncompte.sharepoint.com".
        site_path     : chemin du site, ex. "/sites/PrevoyanceITIP".
        file_path     : chemin du fichier DANS la bibliothèque, ex.
                        "Documents partages/MRM/MRM_Fiab_31_12_23.xlsx".
        dest_path     : destination LOCALE driver, ex. "/dbfs/FileStore/itip/mrm.xlsx".

    Returns:
        dest_path (à passer ensuite à read_excel_to_spark).
    """
    import os
    import requests

    headers = {"Authorization": f"Bearer {token}"}

    # 1. Résoudre l'ID du site SharePoint.
    site = requests.get(
        f"{GRAPH}/sites/{site_hostname}:{site_path}", headers=headers, timeout=30,
    )
    site.raise_for_status()
    site_id = site.json()["id"]

    # 2. Télécharger le contenu du fichier (drive par défaut du site).
    content = requests.get(
        f"{GRAPH}/sites/{site_id}/drive/root:/{file_path}:/content",
        headers=headers, timeout=120,
    )
    content.raise_for_status()

    parent = os.path.dirname(dest_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(dest_path, "wb") as fh:
        fh.write(content.content)
    logger.info("SharePoint → %s (%d octets)", dest_path, len(content.content))
    return dest_path


def read_sharepoint_excel(
    spark        : SparkSession,
    tenant_id    : str,
    client_id    : str,
    client_secret: str,
    site_hostname: str,
    site_path    : str,
    file_path    : str,
    dest_path    : str = "/dbfs/FileStore/itip_fiab_sharepoint/source.xlsx",
    sheet        = 0,
) -> DataFrame:
    """
    Bout-en-bout : authentifie, télécharge le fichier SharePoint, le lit en Spark.

    Renvoie un DataFrame Spark (tout en string) prêt pour clean_*. Penser à
    alimenter client_id/secret depuis dbutils.secrets (cf. docstring du module).
    """
    token = get_graph_token(tenant_id, client_id, client_secret)
    local = download_sharepoint_file(token, site_hostname, site_path, file_path, dest_path)
    return read_excel_to_spark(spark, local, sheet=sheet, as_string=True)
