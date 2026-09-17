"""Validasi schema payload GBFS.

Validasi dipisah dua tingkat supaya satu stasiun rusak tidak menggagalkan
seluruh snapshot:
  1. Envelope (struktur terluar GBFS) — kalau gagal, respons tidak dipercaya -> DLQ.
  2. Per-stasiun — baris rusak dikarantina, stasiun lain tetap diproses.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

__all__ = [
    "GBFSEnvelope",
    "StationInformationEnvelope",
    "StationStatus",
    "ValidationError",
]


class StationStatus(BaseModel):
    """Satu baris status satu stasiun dari GBFS."""

    # GBFS menambah field baru dari waktu ke waktu; jangan gagal karenanya.
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    station_id: str = Field(min_length=1)
    num_bikes_available: int = Field(ge=0)
    num_docks_available: int = Field(ge=0)

    # Tidak selalu ada di semua operator GBFS.
    num_ebikes_available: int | None = Field(default=None, ge=0)
    num_scooters_available: int | None = Field(default=None, ge=0)
    num_bikes_disabled: int | None = Field(default=None, ge=0)
    num_docks_disabled: int | None = Field(default=None, ge=0)

    is_installed: int | bool | None = None
    is_renting: int | bool | None = None
    is_returning: int | bool | None = None
    is_disabled: int | bool | None = None

    last_reported: int | None = Field(
        default=None, description="Unix epoch detik menurut operator."
    )

    @field_validator("station_id", mode="before")
    @classmethod
    def _station_id_to_str(cls, v: Any) -> Any:
        # GBFS kadang mengirim station_id sebagai angka.
        return str(v) if v is not None else v

    @field_validator("is_installed", "is_renting", "is_returning", "is_disabled", mode="before")
    @classmethod
    def _flag_to_bool(cls, v: Any) -> Any:
        if v is None or isinstance(v, bool):
            return v
        return bool(v)

    @property
    def last_reported_dt(self) -> datetime | None:
        """``last_reported`` sebagai datetime UTC tanpa timezone info."""
        if self.last_reported is None:
            return None
        return datetime.fromtimestamp(self.last_reported, tz=timezone.utc).replace(tzinfo=None)


class _GBFSData(BaseModel):
    model_config = ConfigDict(extra="ignore")
    stations: list[dict]


class GBFSEnvelope(BaseModel):
    """Struktur terluar respons station_status."""

    model_config = ConfigDict(extra="ignore")

    last_updated: int | None = None
    ttl: int | None = None
    data: _GBFSData

    @property
    def last_updated_dt(self) -> datetime | None:
        if self.last_updated is None:
            return None
        return datetime.fromtimestamp(self.last_updated, tz=timezone.utc).replace(tzinfo=None)


class StationInformationEnvelope(BaseModel):
    """Struktur terluar respons station_information."""

    model_config = ConfigDict(extra="ignore")

    last_updated: int | None = None
    ttl: int | None = None
    data: _GBFSData