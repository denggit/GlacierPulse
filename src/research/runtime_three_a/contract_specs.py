from __future__ import annotations

from dataclasses import dataclass


KNOWN_OKX_CONTRACT_MULTIPLIERS = {
    "ETH-USDT-SWAP": 0.1,
}


@dataclass(frozen=True)
class ContractMultiplierResolution:
    multiplier: float
    source: str


def resolve_contract_multiplier(
    symbol: str,
    explicit_multiplier: float | None = None,
) -> ContractMultiplierResolution:
    if explicit_multiplier is not None:
        multiplier = float(explicit_multiplier)
        if multiplier <= 0:
            raise ValueError(f"contract multiplier must be positive: {multiplier}")
        return ContractMultiplierResolution(
            multiplier=multiplier,
            source="cli_explicit",
        )

    normalized_symbol = str(symbol).upper()
    known_multiplier = KNOWN_OKX_CONTRACT_MULTIPLIERS.get(normalized_symbol)
    if known_multiplier is not None:
        multiplier = float(known_multiplier)
        if multiplier <= 0:
            raise ValueError(f"contract multiplier must be positive: {multiplier}")
        return ContractMultiplierResolution(
            multiplier=multiplier,
            source="okx_known_default",
        )

    if "-SWAP" in normalized_symbol:
        raise ValueError(
            "contract multiplier is required for unknown SWAP symbol: "
            f"{symbol}. Known default exists for ETH-USDT-SWAP=0.1."
        )

    return ContractMultiplierResolution(
        multiplier=1.0,
        source="non_swap_default",
    )
