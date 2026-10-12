"""SERVE: run every query in sql/reports/ and print the results.

    python -m pipeline.report
"""
from pipeline import config
from pipeline.db import connect_warehouse


def print_table(headers, rows):
    cells = [[("" if value is None else str(value)) for value in row] for row in rows]
    widths = [max([len(header)] + [len(row[i]) for row in cells]) for i, header in enumerate(headers)]
    print("  " + "  ".join(h.ljust(w) for h, w in zip(headers, widths)))
    print("  " + "  ".join("-" * w for w in widths))
    for row in cells:
        print("  " + "  ".join(v.ljust(w) for v, w in zip(row, widths)))


def run():
    with connect_warehouse() as conn:
        for script in sorted((config.SQL_DIR / "reports").glob("*.sql")):
            query = script.read_text(encoding="utf-8")
            title = query.splitlines()[0].lstrip("- ").strip()
            cur = conn.execute(query)
            print(f"\n{title}")
            print_table([column.name for column in cur.description], cur.fetchall())


if __name__ == "__main__":
    run()