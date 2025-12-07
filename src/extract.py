from pathlib import Path
import shutil

def run_extract():
    src = Path("data/raw/SAML-D.csv")          # arquivo de entrada
    dst = Path("data/bronze/SAML-D.csv")       # arquivo de saída na bronze

    shutil.copy(src, dst)

    print("Arquivo copiado para a Bronze:", dst.resolve())
    return str(dst)
