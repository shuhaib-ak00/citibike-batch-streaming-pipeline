"""Validasi schema payload GBFS.

Tujuan: menangkap payload rusak atau berubah bentuk SEBELUM masuk pipeline,
supaya masalah skema sumber tidak menyebar ke layer berikutnya.

Validasi dipisah dua tingkat supaya satu stasiun rusak tidak menggagalkan
seluruh snapshot:

  1. **Envelope** (``GBFSEnvelope`` / ``StationInformationEnvelope``) —
     struktur terluar GBFS. Kalau ini gagal, seluruh respons tidak bisa
     dipercaya sehingga dikirim ke Dead Letter Queue.
  2. **Per-stasiun** (``StationStatus`` / ``StationInformation``) — satu baris
     rusak dikarantina (dihitung lalu dikirim ke DLQ) sementara stasiun lain
     tetap diproses.

Payload yang gagal tidak dibuang: prinsipnya data apa pun yang gagal tetap
bisa diaudit.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

__all__ = [
    "GBFSEnvelope",
    "StationInformationEnvelope",
    "StationInformation",
    "StationStatus",
    "ValidationError",
]


class StationStatus(BaseModel):
    """Satu baris status satu stasiun dari GBFS station_status."""

    # GBFS menambah field baru dari waktu ke waktu; jangan gagal karenanya.
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    station_id: str = Field(min_length=1)
    num_bikes_available: int = Field(ge=0)
    num_docks_available: int = Field(ge=0)

    # Tidak selalu ada di semua operator GBFS -> opsional.
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

    @field_validator(
        "is_installed", "is_renting", "is_returning", "is_disabled", mode="before"
    )
    @classmethod
    def _flag_to_bool(cls, v: Any) -> Any:
        # GBFS memakai 0/1, tapi sebagian sumber mengirim true/false.
        if v is None or isinstance(v, bool):
            return v
        return bool(v)

    @property
    def last_reported_dt(self) -> datetime | None:
        """``last_reported`` sebagai datetime UTC tanpa tzinfo."""
        if self.last_reported is None:
            return None
        return (
            datetime.fromtimestamp(self.last_reported, tz=timezone.utc)
            .replace(tzinfo=None)
        )


class StationInformation(BaseModel):
    """Satu stasiun dari GBFS station_information (referensi semi-statis)."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    station_id: str = Field(min_length=1)
    name: str | None = None
    short_name: str | None = None
    lat: float | None = None
    lon: float | None = None
    capacity: int | None = None
    region_id: str | None = None

    rental_methods: list[str] | None = None
    eightd_has_key_dispenser: bool | None = None

    @field_validator("station_id", "region_id", mode="before")
    @classmethod
    def _to_str(cls, v: Any) -> Any:
        return str(v) if v is not None else v


class _GBFSData(BaseModel):
    model_config = ConfigDict(extra="ignore")
    stations: list[dict]


class _GBFSEnvelopeBase(BaseModel):
    """Bagian envelope GBFS yang sama untuk semua feed.

    Disebut "base" karena struktur terluar GBFS konsisten antar feed:
    ``last_updated``, ``ttl``, dan ``data.stations``.
    """

    model_config = ConfigDict(extra="ignore")

    last_updated: int | None = None
    ttl: int | None = None
    data: _GBFSData

    @property
    def last_updated_dt(self) -> datetime | None:
        """``last_updated`` sebagai datetime UTC tanpa tzinfo."""
        if self.last_updated is None:
            return None
        return (
            datetime.fromtimestamp(self.last_updated, tz=timezone.utc)
            .replace(tzinfo=None)
        )


class GBFSEnvelope(_GBFSEnvelopeBase):
    """Struktur terluar respons station_status."""


class StationInformationEnvelope(_GBFSEnvelopeBase):
    """Struktur terluar respons station_information."""
