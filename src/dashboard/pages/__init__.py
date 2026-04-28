"""Dashboard page registry to keep app routing minimal and stable."""

from dataclasses import dataclass
from typing import Callable

from src.dashboard.pages import (
    bank_rankings,
    banking_trends,
    cbu_april_test,
    cbu_bankstats_ytd,
    data_catalogue,
    deposits_loans,
    executive,
    payments_digital,
    regional_analysis,
)


@dataclass(frozen=True)
class PageSpec:
    """Defines one dashboard tab and how to render it."""

    title: str
    render: Callable[[dict], None]


PAGE_SPECS = [
    PageSpec("Executive overview", lambda ctx: executive.render(ctx["bank_filtered"])),
    PageSpec(
        "Banking sector trends",
        lambda ctx: banking_trends.render(ctx["bank_filtered"], ctx["selected_indicator"]),
    ),
    PageSpec("Bank rankings", lambda ctx: bank_rankings.render(ctx["bank_filtered"])),
    PageSpec(
        "Regional analysis",
        lambda ctx: regional_analysis.render(ctx["region_filtered"], ctx["selected_indicator"]),
    ),
    PageSpec("Deposits and loans", lambda ctx: deposits_loans.render(ctx["bank_filtered"])),
    PageSpec(
        "Payments and digital finance",
        lambda ctx: payments_digital.render(ctx["bank_filtered"], ctx["region_filtered"]),
    ),
    PageSpec("Data catalogue", lambda ctx: data_catalogue.render(ctx["catalog_df"])),
    PageSpec("CBU Banking Stats Test", lambda _ctx: cbu_april_test.render()),
    PageSpec("CBU Banking Stats YTD", lambda _ctx: cbu_bankstats_ytd.render()),
]
