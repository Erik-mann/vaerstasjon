from __future__ import annotations
from pathlib import Path
import csv

BASE = Path(__file__).resolve().parent.parent
MANUAL = BASE / "manuelt"
SNOW = MANUAL / "sno.csv"

def read_existing() -> dict[str, str]:
    if not SNOW.exists():
        return {}
    data: dict[str, str] = {}
    with SNOW.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            d = (row.get("Date") or "").strip()
            v = (row.get("Snow_cm") or "").strip()
            if d:
                data[d] = v
    return data

def write_all(data: dict[str, str]) -> None:
    MANUAL.mkdir(parents=True, exist_ok=True)
    with SNOW.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["Date", "Snow_cm"])
        w.writeheader()
        for d in sorted(data.keys()):
            w.writerow({"Date": d, "Snow_cm": data[d]})

def ask_date() -> str:
    year = input("År (YYYY): ").strip()
    month = input("Måned (1-12): ").strip().zfill(2)
    day = input("Dag (1-31): ").strip().zfill(2)
    return f"{year}-{month}-{day}"

def ask_snow() -> str:
    s = input("Snødybde i cm (bruk punktum, f.eks. 12.4): ").strip()
    s = s.replace(",", ".")  # tåler også komma, men lagrer med punktum
    float(s)  # valider
    return s

def main() -> None:
    data = read_existing()

    while True:
        try:
            d = ask_date()
            v = ask_snow()
        except ValueError:
            print("Ugyldig snøverdi. Bruk tall, f.eks. 12.4")
            continue

        # Overskriv dato hvis den finnes
        data[d] = v
        write_all(data)
        print(f"Lagra: {d} = {v} cm  (fil: {SNOW})")

        more = input("Registrere flere målinger? (j/n): ").strip().lower()
        if more not in ("j", "ja", "y", "yes"):
            break

if __name__ == "__main__":
    main()
