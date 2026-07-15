"""
Lecture de sources externes : fichiers Excel et SharePoint (Databricks).

POURQUOI CE MODULE : Spark ne lit pas nativement le .xlsx (contrairement au CSV
et au Parquet), et un fichier posé sur SharePoint n'est pas accessible depuis le
cluster. Ce module fournit les deux ponts, en lisant TOUT EN STRING — les casts
ciblés sont faits ensuite dans clean_* (cf. load_data), pour ne jamais dépendre
de types devinés à la lecture.

LIRE UN EXCEL — deux voies :
  1. pandas + openpyxl (read_excel_to_spark) — driver-side, suffisant jusqu'à
     ~1M cellules (un inventaire MRM tient large). openpyxl est déjà dans le
     Databricks Runtime : rien à installer. C'est la voie utilisée par défaut.
  2. spark-excel (read_excel_spark_native) — lecture distribuée, réservée aux
     très gros classeurs. Exige la lib Maven com.crealytics:spark-excel_2.12
     sur le cluster (Cluster ▸ Libraries ▸ Install ▸ Maven).

SHAREPOINT — voie DÉSACTIVÉE aujourd'hui (SHAREPOINT["actif"] = False dans
config/profile.py) : le fichier MRM est déposé à la main sur DBFS. Le code
ci-dessous reste en place pour une réactivation ultérieure.

Il n'existe pas de montage SharePoint fiable depuis le cluster ; la voie robuste
est l'API Microsoft Graph (client credentials) : on récupère le fichier en
octets, on l'écrit sur DBFS, puis on le lit comme un Excel local.
Prérequis IT, à faire une fois côté Azure :
    - app registration Azure AD → tenant_id, client_id, client_secret ;
    - permission application Graph « Sites.Read.All » (consentement admin) ;
    - secret d'app dans un secret scope Databricks, JAMAIS en dur dans le code :
        dbutils.secrets.get("itip", "sp_client_secret").
"""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

GRAPH = "https://graph.microsoft.com/v1.0"

# Schéma de chemin déclenchant la voie SharePoint dans resolve_source_path :
#   "sharepoint:/Documents partages/MRM/MRM_Fiab_31_12_24.xlsx"
SHAREPOINT_SCHEME = "sharepoint:"


def _to_local(path: str) -> str:
    """Convertit un chemin dbfs:/... en /dbfs/... (writers/readers driver-side)."""
    return path.replace("dbfs:/", "/dbfs/", 1) if path.startswith("dbfs:/") else path


def is_sharepoint_path(path) -> bool:
    """True si le chemin source désigne un fichier SharePoint (schéma sharepoint:)."""
    return bool(path) and str(path).lower().startswith(SHAREPOINT_SCHEME)


def resolve_source_path(
    spark      : SparkSession,
    path       : str,
    sp_cfg     : dict,
    staging_dir: str,
) -> str:
    """
    Résout un chemin source vers un chemin lisible par Spark.

    "sharepoint:/<chemin dans la bibliothèque>" → téléchargement Microsoft
    Graph vers <staging_dir>/<nom du fichier> (DBFS) ; tout autre chemin
    (dbfs:/, abfss:/, local) est renvoyé tel quel.

    La voie SharePoint n'est empruntée que si SHAREPOINT["actif"] est True.
    Désactivée (mode actuel : dépôt manuel du fichier sur DBFS), un chemin
    "sharepoint:/..." est refusé explicitement — mieux vaut un run rouge
    qu'une source silencieusement absente.

    Args:
        spark       : SparkSession active (résolution du secret via dbutils).
        path        : chemin source (INVENTAIRES / widget fichier_mrm*).
        sp_cfg      : config SHAREPOINT (actif, tenant_id, client_id, hostname,
                      site, client_secret OU secret_scope+secret_key).
        staging_dir : dossier DBFS de dépôt des téléchargements.

    Returns:
        Chemin Spark (dbfs:/...) du fichier téléchargé, ou `path` inchangé.

    Raises:
        ValueError si le chemin est SharePoint alors que la voie est désactivée,
        ou si elle est active mais la config incomplète.
    """
    if not is_sharepoint_path(path):
        return path

    if not sp_cfg.get("actif"):
        raise ValueError(
            f"Chemin SharePoint '{path}' alors que la voie SharePoint est "
            "désactivée (SHAREPOINT['actif'] = False dans config/profile.py). "
            "Déposer le fichier à la main sur DBFS et pointer INVENTAIRES sur "
            "son chemin dbfs:/ — ou réactiver la voie (cf. config/profile.py)."
        )

    manquants = [k for k in ("tenant_id", "client_id", "hostname", "site")
                 if not sp_cfg.get(k)]
    if manquants:
        raise ValueError(
            f"Chemin SharePoint '{path}' mais config SHAREPOINT incomplète "
            f"(champs vides : {', '.join(manquants)}) — cf. config/profile.py."
        )

    secret = sp_cfg.get("client_secret")
    if not secret and sp_cfg.get("secret_scope"):
        from pyspark.dbutils import DBUtils  # dispo sur cluster Databricks uniquement
        secret = DBUtils(spark).secrets.get(sp_cfg["secret_scope"], sp_cfg["secret_key"])
    if not secret:
        raise ValueError(
            "Secret SharePoint introuvable : renseigner SHAREPOINT['secret_scope'/"
            "'secret_key'] (secret scope Databricks) ou 'client_secret'."
        )

    file_path = path[len(SHAREPOINT_SCHEME):].lstrip("/")
    dest_dbfs = f"{staging_dir.rstrip('/')}/{file_path.rsplit('/', 1)[-1]}"
    token = get_graph_token(sp_cfg["tenant_id"], sp_cfg["client_id"], secret)
    download_sharepoint_file(
        token, sp_cfg["hostname"], sp_cfg["site"], file_path, _to_local(dest_dbfs),
    )
    return dest_dbfs


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
        path      : chemin du fichier — dbfs:/... accepté (converti en /dbfs/...),
                    sinon chemin local du driver.
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
        _to_local(path), sheet_name=sheet, header=header,
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
