#!/usr/bin/env python3
"""Small, dependency-free data pipeline used by the GitHub Actions example."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


class PipelineError(Exception):
    """An actionable error in the source data or pipeline configuration."""


def read_source(path: Path) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise PipelineError(f"Arquivo de entrada não encontrado: {path}") from exc
    except json.JSONDecodeError as exc:
        raise PipelineError(f"JSON inválido em {path}, linha {exc.lineno}: {exc.msg}") from exc
    except OSError as exc:
        raise PipelineError(f"Não foi possível ler {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise PipelineError("A raiz do JSON precisa ser um objeto.")
    products = data.get("products")
    orders = data.get("orders")
    if not isinstance(products, list) or not isinstance(orders, list):
        raise PipelineError("O JSON precisa conter as listas 'products' e 'orders'.")

    product_by_sku: dict[str, dict[str, Any]] = {}
    for index, product in enumerate(products, start=1):
        if not isinstance(product, dict):
            raise PipelineError(f"Produto {index} precisa ser um objeto.")
        sku = product.get("sku")
        name = product.get("name")
        category = product.get("category")
        price = product.get("unit_price")
        if not all(isinstance(value, str) and value for value in (sku, name, category)):
            raise PipelineError(f"Produto {index} precisa ter sku, name e category.")
        if not isinstance(price, (int, float)) or isinstance(price, bool) or price < 0:
            raise PipelineError(f"Preço inválido para o produto {sku}.")
        if sku in product_by_sku:
            raise PipelineError(f"SKU duplicado: {sku}.")
        product_by_sku[sku] = product

    return product_by_sku, orders


def merge_orders(
    product_by_sku: dict[str, dict[str, Any]], orders: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Join each order row with its product using the shared SKU key."""
    merged: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for index, order in enumerate(orders, start=1):
        if not isinstance(order, dict):
            raise PipelineError(f"Pedido {index} precisa ser um objeto.")
        order_id = order.get("order_id")
        sku = order.get("sku")
        quantity = order.get("quantity")
        status = order.get("status")
        if not isinstance(order_id, str) or not isinstance(sku, str):
            raise PipelineError(f"Pedido {index} precisa ter order_id e sku.")
        if not isinstance(quantity, int) or isinstance(quantity, bool):
            raise PipelineError(f"Quantidade inválida no pedido {order_id}.")
        if not isinstance(status, str):
            raise PipelineError(f"Status inválido no pedido {order_id}.")

        product = product_by_sku.get(sku)
        if product is None:
            rejected.append({"order_id": order_id, "sku": sku, "reason": "SKU não encontrado"})
            continue
        unit_price = float(product["unit_price"])
        merged.append(
            {
                "order_id": order_id,
                "sku": sku,
                "product": product["name"],
                "category": product["category"],
                "quantity": quantity,
                "status": status,
                "unit_price": round(unit_price, 2),
                "line_total": round(unit_price * quantity, 2),
            }
        )
    return merged, rejected


def filter_paid(merged: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Keep paid orders with a positive quantity and explain filtered rows."""
    included: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for row in merged:
        reason = None
        if row["quantity"] <= 0:
            reason = "quantidade não positiva"
        elif row["status"] != "paid":
            reason = "pedido ainda não pago"
        if reason:
            excluded.append({**row, "reason": reason})
        else:
            included.append(row)
    return included, excluded


def split_batches(rows: list[dict[str, Any]], size: int = 2) -> list[list[dict[str, Any]]]:
    return [rows[start : start + size] for start in range(0, len(rows), size)]


def summarize(rows: list[dict[str, Any]], batch_count: int, rejected_count: int) -> dict[str, Any]:
    by_category: dict[str, dict[str, Any]] = defaultdict(lambda: {"items": 0, "units": 0, "revenue": 0.0})
    for row in rows:
        group = by_category[row["category"]]
        group["items"] += 1
        group["units"] += row["quantity"]
        group["revenue"] = round(group["revenue"] + row["line_total"], 2)
    return {
        "filtered_orders": len(rows),
        "units": sum(row["quantity"] for row in rows),
        "revenue": round(sum(row["line_total"] for row in rows), 2),
        "batch_count": batch_count,
        "rejected_orders": rejected_count,
        "by_category": dict(sorted(by_category.items())),
    }


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run(input_path: Path, output_path: Path) -> dict[str, Any]:
    products, orders = read_source(input_path)
    merged, rejected = merge_orders(products, orders)
    filtered, excluded = filter_paid(merged)
    batches = split_batches(filtered)
    summary = summarize(filtered, len(batches), len(rejected))

    write_json(output_path / "merged.json", merged)
    write_json(output_path / "filtered.json", filtered)
    write_json(output_path / "filtered-out.json", excluded)
    write_json(output_path / "rejected.json", rejected)
    write_json(output_path / "summary.json", summary)
    for index, batch in enumerate(batches, start=1):
        write_json(output_path / "batches" / f"part-{index:03d}.json", batch)

    lines = [
        "# Resumo do pipeline de dados",
        "",
        f"- Pedidos pagos incluídos: {summary['filtered_orders']}",
        f"- Unidades: {summary['units']}",
        f"- Receita: R$ {summary['revenue']:.2f}",
        f"- Partes geradas: {summary['batch_count']}",
        f"- Pedidos rejeitados por SKU desconhecido: {summary['rejected_orders']}",
        "",
        "| Categoria | Pedidos | Unidades | Receita |",
        "| --- | ---: | ---: | ---: |",
    ]
    for category, values in summary["by_category"].items():
        lines.append(
            f"| {category} | {values['items']} | {values['units']} | R$ {values['revenue']:.2f} |"
        )
    output_path.mkdir(parents=True, exist_ok=True)
    (output_path / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("sample_orders.json"))
    parser.add_argument("--output", type=Path, default=Path("build/data-pipeline"))
    args = parser.parse_args()
    try:
        summary = run(args.input, args.output)
    except (PipelineError, OSError) as exc:
        print(f"Erro no pipeline: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
