"""Garde-fou structurel : le TypedDict SyntheseScalars == les clés réellement
renvoyées par _scalars_from_rows. Test PUR (AST + typing, sans Spark) → tourne
partout. Si quelqu'un ajoute/retire une clé du return sans mettre à jour le
TypedDict (ou l'inverse), ce test échoue."""

import ast
import pathlib

from core.synthese.synthese_contract import SyntheseScalars

SCALARS = (pathlib.Path(__file__).resolve().parents[1]
           / "core" / "synthese" / "synthese_scalars.py")


def _return_keys_of_scalars_from_rows():
    tree = ast.parse(SCALARS.read_text(encoding="utf-8"))
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "_scalars_from_rows")
    ret = fn.body[-1]                          # le return final, au top-level
    assert isinstance(ret, ast.Return) and isinstance(ret.value, ast.Dict)
    return [k.value for k in ret.value.keys]


def test_typeddict_couvre_exactement_le_return():
    returned = _return_keys_of_scalars_from_rows()
    declared = set(SyntheseScalars.__annotations__)

    assert len(returned) == len(set(returned)), "clé dupliquée dans le return"
    assert set(returned) == declared, {
        "absentes_du_TypedDict": sorted(set(returned) - declared),
        "en_trop_dans_TypedDict": sorted(declared - set(returned)),
    }
